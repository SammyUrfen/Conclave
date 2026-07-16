package media

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media/ivfwriter"
)

// RecordVP8 writes a remote VP8 track to w as an IVF file until the track ends
// (the PeerConnection closes, which unblocks the read) and returns the number of
// RTP packets written. If the sender's payload is real VP8, the resulting file
// is playable; with synthetic frames it is a faithful record of a non-decodable
// stream — either way it proves the packets arrived.
func RecordVP8(log *slog.Logger, track *webrtc.TrackRemote, w io.Writer) (int, error) {
	writer, err := ivfwriter.NewWith(w) // default codec is VP8
	if err != nil {
		return 0, fmt.Errorf("new ivf writer: %w", err)
	}
	defer writer.Close()

	n := 0
	for {
		pkt, _, err := track.ReadRTP()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return n, nil
			}
			return n, err
		}
		if err := writer.WriteRTP(pkt); err != nil {
			return n, fmt.Errorf("write rtp: %w", err)
		}
		n++
		if n%100 == 0 {
			log.Info("recording media", slog.Int("packets", n))
		}
	}
}

// DrainAndCount reads and discards a remote track, returning the packet count
// when it ends. Used when we only need to prove media arrived, not keep it.
func DrainAndCount(log *slog.Logger, track *webrtc.TrackRemote) (int, error) {
	n := 0
	for {
		if _, _, err := track.ReadRTP(); err != nil {
			if errors.Is(err, io.EOF) {
				return n, nil
			}
			return n, err
		}
		n++
		if n%100 == 0 {
			log.Info("received media", slog.Int("packets", n))
		}
	}
}
