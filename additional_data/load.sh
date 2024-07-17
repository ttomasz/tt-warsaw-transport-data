gcloud auth login

gcloud config set project warszawski-transport

bq load \
 --source_format=NEWLINE_DELIMITED_JSON \
 --json_extension=GEOJSON \
 --autodetect \
 warsaw_transport.excluded_areas \
 excluded_areas.geojsonl
