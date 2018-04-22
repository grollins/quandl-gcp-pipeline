#!/bin/bash
cp output/`date --date="yesterday" +"%Y-%m-%d"`.png web/img/panel1.png
cd web
gcloud app deploy --quiet
