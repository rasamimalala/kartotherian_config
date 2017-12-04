bash script to update the osm data with diffs

if the script is run behind a proxy you need to set them for osmosis:

`JAVACMD_OPTIONS="-Dhttp.proxyHost=<proxy> -Dhttp.proxyPort=<port> -Dhttps.proxyHost=<proxy> -Dhttps.proxyPort=<port>" osm_update.sh`