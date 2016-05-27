pkill -9 firefox
pkill -9 Xvfb
find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf
python crawler_generic.py 4000 2
find /tmp/* -maxdepth 1 -type d -name 'tmp*' |  xargs rm -rf
