package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

// Test-local record kinds — the generic WAL does not define any; consumers do.
const (
	kCmd   Kind = 1
	kTrade Kind = 2
	kExec  Kind = 3
)

func TestFrameRoundtrip(t *testing.T) {
	cases := []struct {
		name    string
		seqNo   uint64
		kind    Kind
		payload []byte
	}{
		{"empty", 1, kCmd, []byte{}},
		{"small", 2, kTrade, []byte("hello")},
		{"medium", 3, kExec, bytes.Repeat([]byte{0xAB}, 1024)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := EncodeFrame(nil, tc.seqNo, tc.kind, tc.payload)
			f, _, err := ReadFrame(bytes.NewReader(buf), nil)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if f.SeqNo != tc.seqNo || f.Kind != tc.kind || !bytes.Equal(f.Payload, tc.payload) {
				t.Fatalf("mismatch: got seq=%d kind=%d len=%d, want seq=%d kind=%d len=%d",
					f.SeqNo, f.Kind, len(f.Payload), tc.seqNo, tc.kind, len(tc.payload))
			}
		})
	}
}

func TestFrameCRCDetectsBitflip(t *testing.T) {
	buf := EncodeFrame(nil, 42, kCmd, []byte("payload"))
	// Flip one bit in the payload.
	buf[len(buf)-1] ^= 0xFF
	if _, _, err := ReadFrame(bytes.NewReader(buf), nil); err != ErrBadCRC {
		t.Fatalf("expected ErrBadCRC, got %v", err)
	}
}

func TestFrameTruncated(t *testing.T) {
	buf := EncodeFrame(nil, 42, kCmd, []byte("payload"))
	// Drop last byte.
	short := buf[:len(buf)-1]
	if _, _, err := ReadFrame(bytes.NewReader(short), nil); err != ErrTruncated {
		t.Fatalf("expected ErrTruncated, got %v", err)
	}
}

func TestFrameEOFAtBoundary(t *testing.T) {
	if _, _, err := ReadFrame(bytes.NewReader(nil), nil); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestReadFrameBadPayloadLen(t *testing.T) {
	buf := EncodeFrame(nil, 42, kCmd, []byte("small"))
	// Corrupt the length field to exceed MaxPayloadBytes — ReadFrame must
	// refuse to allocate rather than trust the header.
	binary.LittleEndian.PutUint32(buf[0:4], MaxPayloadBytes+1)
	if _, _, err := ReadFrame(bytes.NewReader(buf), nil); err != ErrBadPayloadLen {
		t.Fatalf("expected ErrBadPayloadLen, got %v", err)
	}
}

func TestFrameMaxPayloadBoundary(t *testing.T) {
	big := bytes.Repeat([]byte{0xCD}, MaxPayloadBytes)
	buf := EncodeFrame(nil, 100, kExec, big)
	f, _, err := ReadFrame(bytes.NewReader(buf), nil)
	if err != nil {
		t.Fatalf("read at MaxPayloadBytes boundary: %v", err)
	}
	if len(f.Payload) != MaxPayloadBytes || !bytes.Equal(f.Payload, big) {
		t.Fatalf("boundary roundtrip mismatch: got len=%d", len(f.Payload))
	}
}
