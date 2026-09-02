#!/usr/bin/env bash
# make-layers.sh — generate the three VP8 IVF quality layers a conclave peer can publish.
#
# WHY A SCRIPT AND NOT COMMITTED FILES. The layers are build artifacts of a source
# clip, they are megabytes, and this repository is trying to stop committing binaries
# (see .gitignore's *.ivf rule, which already covers the output). A script is
# reproducible, reviewable and 2 KB.
#
# WHAT THIS DEMONSTRATES, AND WHAT IT DOES NOT. conclave has NO ENCODER: an origin
# plays a pre-encoded file. Three files therefore stand in for three live encodings.
# That is enough to demonstrate layer SELECTION — the relay choosing per child, which
# is the novel part — and it is not layer PRODUCTION. A real sender would run one
# encoder per rung and adapt each one's bitrate; nothing here does that.
#
# Usage:
#   scripts/make-layers.sh [SOURCE.mp4|SOURCE.ivf] [OUTDIR]
#
# With no SOURCE it synthesises one with ffmpeg's own test pattern, so the script
# needs nothing but ffmpeg to run.
set -euo pipefail

SRC="${1:-}"
OUT="${2:-.}"
DUR="${LAYER_SECONDS:-10}"
FPS="${LAYER_FPS:-30}"

command -v ffmpeg >/dev/null || { echo "make-layers.sh: ffmpeg not found" >&2; exit 1; }
mkdir -p "$OUT"

if [[ -z "$SRC" ]]; then
  SRC="$OUT/layer-src.mp4"
  echo "no source given; synthesising $SRC"
  # A moving pattern with a burned-in resolution label, so a recorded layer is
  # identifiable BY EYE as well as by ffprobe.
  ffmpeg -y -loglevel error \
    -f lavfi -i "testsrc=duration=${DUR}:size=1280x720:rate=${FPS}" \
    -c:v libx264 -pix_fmt yuv420p "$SRC"
fi

# The three rungs. Resolution AND bitrate scale together, which is what makes the
# layers visibly different in a recording rather than merely differently compressed:
#   q (quarter) 320x180   @  150 kbit/s
#   h (half)    640x360   @  500 kbit/s
#   f (full)   1280x720   @ 1500 kbit/s
#
# -deadline realtime + -cpu-used 4 keeps libvpx from spending minutes on a demo clip;
# -g 30 puts a keyframe every second, which matters here: a relay that switches a
# child to a new layer DROPS UNTIL THAT LAYER'S NEXT KEYFRAME, so a long GOP would
# make every switch look like a multi-second freeze that is the clip's fault, not the
# relay's.
emit() {
  local name="$1" scale="$2" rate="$3"
  echo "encoding $OUT/layer-$name.ivf  ($scale @ $rate)"
  ffmpeg -y -loglevel error -i "$SRC" \
    -c:v libvpx -b:v "$rate" -vf "scale=$scale" -r "$FPS" -g 30 \
    -deadline realtime -cpu-used 4 -an -f ivf "$OUT/layer-$name.ivf"
}

emit q 320:180  150k
emit h 640:360  500k
emit f 1280:720 1500k

echo
echo "layers written to $OUT:"
ls -l "$OUT"/layer-q.ivf "$OUT"/layer-h.ivf "$OUT"/layer-f.ivf
echo
echo "publish them with:"
echo "  peer -call -send -media $OUT/layer-q.ivf,$OUT/layer-h.ivf,$OUT/layer-f.ivf ..."
