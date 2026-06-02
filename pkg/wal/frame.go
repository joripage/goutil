// Package wal implements a generic Write-Ahead Log: an append-only,
// length-framed binary log with monotonic sequence numbers, group commit, and
// ack-after-flush semantics.
//
// The log is payload-agnostic — it stores raw []byte records tagged with a
// caller-defined Kind. Consumers decide what each Kind means and how payloads
// are (de)serialised; the WAL only guarantees framing, ordering, integrity
// (crc32c) and durability.
//
// Typical use:
//
//	w, _ := wal.Open(wal.Options{Dir: "./data/wal"})
//	seq, durable, _ := w.Append(myKind, rawBytes)
//	<-durable // record is on stable storage (ack-after-flush)
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// Kind is a caller-defined record tag persisted in every frame. The WAL does
// not interpret it — consumers define their own constants (e.g. record types)
// and switch on Kind when replaying. Values are persisted; pick stable numbers.
type Kind uint8

// FrameHeaderSize is the fixed-size header preceding every payload:
//
//	uint32 length, uint32 crc32, uint64 seqNo, uint8 kind = 17 bytes.
const FrameHeaderSize = 17

// MaxPayloadBytes caps an individual payload — guards against a corrupt header
// asking us to allocate gigabytes. 16 MiB is far above any realistic WAL record.
const MaxPayloadBytes = 16 << 20

// crcTable is the Castagnoli polynomial table — the same `crc32c` used by
// btrfs, ext4 metadata and many other databases. The fmt is shared across
// writers and readers.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrBadCRC is returned when a frame's CRC does not match its body — almost
// certainly a torn write from a crash. Recovery truncates here.
var ErrBadCRC = errors.New("wal: frame crc mismatch (torn write?)")

// ErrTruncated is returned when EOF lands inside a frame.
var ErrTruncated = errors.New("wal: truncated frame")

// ErrBadPayloadLen reports a length field larger than MaxPayloadBytes.
var ErrBadPayloadLen = errors.New("wal: payload length out of range")

// EncodeFrame writes one frame (length + crc + seqNo + kind + payload) into
// the supplied buffer and returns it. Length-prefixed framing means a reader
// can recover at frame boundaries even if the previous record is corrupted.
//
// The CRC covers seqNo, kind, and payload — the same fields that survive
// reading the header. The length field itself is excluded so a writer cannot
// poison the CRC by lying about length: a wrong length is caught when crc fails.
func EncodeFrame(buf []byte, seqNo uint64, kind Kind, payload []byte) []byte {
	if len(payload) > MaxPayloadBytes {
		// Caller is responsible for not exceeding this — but encode is total.
		payload = payload[:MaxPayloadBytes]
	}
	total := FrameHeaderSize + len(payload)
	if cap(buf) < total {
		buf = make([]byte, 0, total)
	}
	buf = buf[:total]

	// CRC body = seqNo (8B) + kind (1B) + payload
	hasher := crc32.New(crcTable)
	var tmp [9]byte
	binary.LittleEndian.PutUint64(tmp[:8], seqNo)
	tmp[8] = byte(kind)
	hasher.Write(tmp[:])
	hasher.Write(payload)
	crc := hasher.Sum32()

	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc)
	binary.LittleEndian.PutUint64(buf[8:16], seqNo)
	buf[16] = byte(kind)
	copy(buf[17:], payload)
	return buf
}

// Frame is a decoded record returned by a Reader.
type Frame struct {
	SeqNo   uint64
	Kind    Kind
	Payload []byte // owned by the reader's buffer — caller must copy to retain
}

// ReadFrame reads one frame from r. It returns io.EOF cleanly at a frame
// boundary, ErrTruncated if EOF lands mid-frame, ErrBadCRC on integrity
// failure, and ErrBadPayloadLen on an unreasonable length. `dst` is reused if
// it has enough capacity, otherwise a new buffer is allocated.
func ReadFrame(r io.Reader, dst []byte) (Frame, []byte, error) {
	var hdr [FrameHeaderSize]byte
	n, err := io.ReadFull(r, hdr[:])
	if err == io.EOF && n == 0 {
		return Frame{}, dst, io.EOF
	}
	if err != nil {
		return Frame{}, dst, ErrTruncated
	}

	plen := binary.LittleEndian.Uint32(hdr[0:4])
	crc := binary.LittleEndian.Uint32(hdr[4:8])
	seqNo := binary.LittleEndian.Uint64(hdr[8:16])
	kind := Kind(hdr[16])

	if plen > MaxPayloadBytes {
		return Frame{}, dst, ErrBadPayloadLen
	}

	if cap(dst) < int(plen) {
		dst = make([]byte, plen)
	}
	dst = dst[:plen]
	if _, err := io.ReadFull(r, dst); err != nil {
		return Frame{}, dst, ErrTruncated
	}

	// Verify CRC over the same bytes EncodeFrame hashed.
	hasher := crc32.New(crcTable)
	var tmp [9]byte
	binary.LittleEndian.PutUint64(tmp[:8], seqNo)
	tmp[8] = byte(kind)
	hasher.Write(tmp[:])
	hasher.Write(dst)
	if hasher.Sum32() != crc {
		return Frame{}, dst, ErrBadCRC
	}

	return Frame{SeqNo: seqNo, Kind: kind, Payload: dst}, dst, nil
}
