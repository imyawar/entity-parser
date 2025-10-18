#!/bin/bash
echo removing directory lambda
rm -rf lambda
echo creating directory lambda
mkdir lambda
echo creating parsers directories
mkdir -p lambda/chick_fil_a
mkdir lambda/daves
mkdir lambda/zaxbys
mkdir lambda/wendys
mkdir lambda/hardees
mkdir lambda/carlsjr
mkdir lambda/kfc
mkdir lambda/popeyes
mkdir lambda/raising_canes
mkdir lambda/core_power_yoga
mkdir lambda/solid_core
mkdir lambda/yoga_six
mkdir lambda/orange_theory
mkdir lambda/common
mkdir lambda/data
echo copying required files to relative directories
cp -r chick_fil_a/CFA* lambda/chick_fil_a/
cp -r daves/Daves* lambda/daves
cp -r zaxbys/Zaxbys* lambda/zaxbys
cp -r wendys/Wendy* lambda/wendys/
cp -r hardees/HAR* lambda/hardees
cp -r carlsjr/CJR* lambda/carlsjr
cp -r kfc/KFC* lambda/kfc
cp -r popeyes/Popeyes* lambda/popeyes
cp -r raising_canes/RC* lambda/raising_canes
cp -r core_power_yoga/CPY* lambda/core_power_yoga
cp -r solid_core/SolidCore* lambda/solid_core
cp -r yoga_six/YogaSix* lambda/yoga_six
cp -r orange_theory/Orange* lambda/orange_theory
cp -r common lambda/
cp -r data/cbsa_bounding_boxes.json lambda/data/cbsa_bounding_boxes.json
cp -r data/cbsa_db.csv lambda/data/cbsa_db.csv
cp  lambda_function.py lambda/
echo removing extra files
rm -rf lambda/common/__pycache__
echo creating archive:resturantLambda.zip
cd lambda
zip -r resturantLambda.zip .

cd ..