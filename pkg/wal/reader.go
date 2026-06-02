package wal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

// SegmentReader streams frames out of one segment file. It surfaces the same
// CRC / truncation errors that Frame decoding does so the caller (recovery)
// can decide whether to halt or truncate-at-tail.
type SegmentReader struct {
	f    *os.File
	br   *bufio.Reader
	dst  []byte // reusable payload buffer
	path string
}

// OpenSegment opens an existing segment file for reading.
func OpenSegment(path string) (*SegmentReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: open segment %s: %w", path, err)
	}
	return &SegmentReader{f: f, br: bufio.NewReaderSize(f, 64<<10), path: path}, nil
}

// Next returns the next frame, or io.EOF when the segment is fully consumed.
// The returned Frame.Payload aliases an internal buffer — copy it before the
// next call if it must outlive that call.
func (s *SegmentReader) Next() (Frame, error) {
	fr, dst, err := ReadFrame(s.br, s.dst)
	s.dst = dst
	return fr, err
}

// Close releases the underlying file.
func (s *SegmentReader) Close() error { return s.f.Close() }

// Path returns the on-disk path that was opened — useful in error messages.
func (s *SegmentReader) Path() string { return s.path }

// ScanResult is the outcome of scanning a single segment for recovery.
type ScanResult struct {
	LastSeqNo  uint64 // last valid seqNo seen (0 if none)
	FrameCount uint64
	Truncated  bool // a tail-truncated frame was tolerated and ignored
	BadCRC     bool // a tail CRC mismatch was tolerated and ignored
	Path       string
}

// IsTailFailure returns true when the segment ended on a torn-write — a real
// recovery would truncate the segment at the last good frame and continue.
func (r ScanResult) IsTailFailure() bool { return r.Truncated || r.BadCRC }

// ScanSegment iterates a segment, invoking visit for every frame whose CRC
// matches. A torn write at the tail (ErrTruncated or ErrBadCRC on the LAST
// frame) is tolerated — flagged on the ScanResult, not returned as error —
// because that is the expected outcome of `kill -9` mid-write. Any earlier
// failure is returned as an error so the caller can halt on divergence.
func ScanSegment(path string, visit func(Frame) error) (ScanResult, error) {
	sr, err := OpenSegment(path)
	if err != nil {
		return ScanResult{Path: path}, err
	}
	defer sr.Close()

	res := ScanResult{Path: path}
	for {
		fr, err := sr.Next()
		if errors.Is(err, io.EOF) {
			return res, nil
		}
		if errors.Is(err, ErrTruncated) {
			res.Truncated = true
			return res, nil
		}
		if errors.Is(err, ErrBadCRC) {
			// A bad CRC is only an expected torn tail when nothing follows it.
			// If bytes still remain after the corrupt frame, this is real
			// mid-stream corruption — surface it rather than silently dropping
			// every later (possibly valid) frame.
			if _, peekErr := sr.br.Peek(1); peekErr == nil {
				return res, fmt.Errorf("wal: scan %s: %w", path, ErrBadCRC)
			} else if !errors.Is(peekErr, io.EOF) {
				return res, fmt.Errorf("wal: scan %s: peek after crc: %w", path, peekErr)
			}
			res.BadCRC = true
			return res, nil
		}
		if err != nil {
			return res, fmt.Errorf("wal: scan %s: %w", path, err)
		}
		if err := visit(fr); err != nil {
			return res, err
		}
		res.LastSeqNo = fr.SeqNo
		res.FrameCount++
	}
}

// ScanAll runs ScanSegment over every segment in dir, in ascending segment
// order. It stops early if visit returns an error or a non-tail failure occurs.
// Returns the highest seqNo seen and a slice of per-segment scan results.
func ScanAll(dir string, visit func(Frame) error) (uint64, []ScanResult, error) {
	idx, err := listSegments(dir)
	if err != nil {
		return 0, nil, err
	}
	var last uint64
	var results []ScanResult
	for _, i := range idx {
		res, err := ScanSegment(segmentPath(dir, i), visit)
		results = append(results, res)
		if err != nil {
			return last, results, err
		}
		if res.LastSeqNo > last {
			last = res.LastSeqNo
		}
		// A tail failure on a non-final segment is a real corruption signal,
		// not an expected torn-write. Surface as error.
		if res.IsTailFailure() && i != idx[len(idx)-1] {
			return last, results, fmt.Errorf("wal: mid-stream corruption in %s", res.Path)
		}
	}
	return last, results, nil
}
