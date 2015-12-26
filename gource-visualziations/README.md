# Using gource to visualize a repository's git history
#
# This was used to create video for visualizations such as the one
# at https://www.youtube.com/watch?v=Ihhlo45RUGk (the music and subtitles
# were added in iMovie).
#
# You'll need gource and ffmpeg (available through homebrew on OS X)
#
# Run this from within a repository

gource \
	--key --background 000000 --logo MEDLogoBLK.jpg --file-idle-time 30 --max-file-lag 1 \
	--title "lsst.afw: making of a library" --colour-images -a .1 --user-scale 1 \
	--multi-sampling --bloom-multiplier 1 --bloom-intensity 0.75 --highlight-all-users \
	--stop-at-end --disable-progress --max-user-speed 400  -1280x720 -r 60 -s 0.25 -o - \
| ffmpeg -y -r 60 -f image2pipe -vcodec ppm -i - -vcodec libx264 -preset medium -pix_fmt yuv420p -crf 9 -threads 0 -bf 0 gource.mp4
