package media

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	pmedia "github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/ivfreader"
)

// sampleWriter is the write side of a local track that a media source needs —
// exactly the one method PlayIVF/SendSynthetic call. Declaring the interface here
// (consumer-defined, the way session.go declares Transport) lets the Router slip
// its upload meter in between a source and the real track: both
// *webrtc.TrackLocalStaticSample and the Router's metered wrapper satisfy it, and
// neither the source nor the track knows the meter exists.
type sampleWriter interface {
	WriteSample(pmedia.Sample) error
}

// PlayIVF streams a VP8 IVF file into track, pacing frames at the file's frame
// rate, looping when it reaches the end so a short clip keeps a demo alive. It
// returns when ctx is cancelled (with ctx.Err) or on a read/decode error.
func PlayIVF(ctx context.Context, log *slog.Logger, path string, track sampleWriter) error {
	for {
		n, err := playIVFOnce(ctx, path, track)
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Info("looping media file", slog.String("path", path), slog.Int("frames", n))
	}
}

func playIVFOnce(ctx context.Context, path string, track sampleWriter) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open media %q: %w", path, err)
	}
	defer f.Close()

	reader, header, err := ivfreader.NewWith(f)
	if err != nil {
		return 0, fmt.Errorf("read ivf header %q: %w", path, err)
	}
	// IVF timebase = numerator/denominator seconds per frame.
	frameDur := time.Duration(float64(header.TimebaseNumerator) / float64(header.TimebaseDenominator) * float64(time.Second))
	if frameDur <= 0 {
		frameDur = 33 * time.Millisecond // sane default (~30 fps)
	}
	ticker := time.NewTicker(frameDur)
	defer ticker.Stop()

	n := 0
	for {
		frame, _, err := reader.ParseNextFrame()
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return n, fmt.Errorf("parse ivf frame: %w", err)
		}
		if err := track.WriteSample(pmedia.Sample{Data: frame, Duration: frameDur}); err != nil {
			return n, fmt.Errorf("write sample: %w", err)
		}
		n++
		select {
		case <-ctx.Done():
			return n, ctx.Err()
		case <-ticker.C:
		}
	}
}

// SendSynthetic writes opaque fixed-size "frames" into track at the given fps.
// The bytes are not decodable VP8 — this proves the transport (RTP flows, the far
// side's OnTrack fires) without needing an encoder or a media file. Used by the
// automated test and by `peer -call` when no -media file is given.
func SendSynthetic(ctx context.Context, track sampleWriter, fps int) error {
	if fps <= 0 {
		fps = 30
	}
	dur := time.Second / time.Duration(fps)
	ticker := time.NewTicker(dur)
	defer ticker.Stop()

	frame := make([]byte, 256)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := track.WriteSample(pmedia.Sample{Data: frame, Duration: dur}); err != nil {
				return fmt.Errorf("write synthetic sample: %w", err)
			}
		}
	}
}
