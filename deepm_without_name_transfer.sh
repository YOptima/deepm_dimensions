#!/bin/sh

sudo python3 deepm_transformation.py $2
sudo bq load --source_format CSV --skip_leading_rows 1 --allow_jagged_rows --replace _deepm.$1 $2.csv
sudo rm -rf $2*

sudo python3 deepm_transformation.py $3
sudo bq load --source_format CSV --skip_leading_rows 1 --allow_jagged_rows --replace _deepm.$1_7D $3.csv
sudo rm -rf $3*

sudo python3 deepm_transformation.py $4
sudo bq load --source_format CSV --skip_leading_rows 1 --allow_jagged_rows --replace _deepm.$1_3D $4.csv
sudo rm -rf $4*

sudo python3 deepm_transformation.py $5
sudo bq load --source_format CSV --skip_leading_rows 1 --allow_jagged_rows --replace _deepm.$1_1D $5.csv
sudo rm -rf $5*

sudo python3 deepm_transformation.py $6
sudo bq load --source_format CSV --skip_leading_rows 1 --allow_jagged_rows --replace _deepm.$1_1H $6.csv
sudo rm -rf $6*

sleep 5

sudo bq query --use_legacy_sql=false 'Update `_deepm.'$1'`
Set 
Post_Click_Conversions_7D = 0.0,
Post_View_Conversions_7D = 0.0,
Post_Click_Conversions_3D = 0.0,
Post_View_Conversions_3D = 0.0,
Post_Click_Conversions_1D = 0.0,
Post_Click_Conversions_1H = 0.0,
Post_View_Conversions_1D = 0.0,
Post_View_Conversions_1H = 0.0
Where true'
sleep 5

sudo bq query --use_legacy_sql=false 'UPDATE `_deepm.'$1'` a
    SET a.Post_Click_Conversions_1H = b.Post_Click_Conversions_1H,
    a.Post_View_Conversions_1H = b. Post_View_Conversions_1H
    FROM `_deepm.'$1'_1H` b
    WHERE
    a. Advertiser_Currency = b. Advertiser_Currency AND
    a.Date=b.Date AND
    IFNULL(a. Partner_ID ,0) = IFNULL(b. Partner_ID ,0) AND
    IFNULL(a. Partner ,"") = IFNULL(b. Partner ,"") AND
    IFNULL(a. Advertiser_ID ,0) = IFNULL(b. Advertiser_ID ,0) AND
    IFNULL(a. Advertiser_Status ,"") = IFNULL(b. Advertiser_Status ,"") AND
    IFNULL(a. Line_Item_ID ,0) = IFNULL(b. Line_Item_ID ,0) AND
    IFNULL(a. Line_Item_Status ,"") = IFNULL(b. Line_Item_Status ,"") AND
    IFNULL(a. Line_Item_Type ,"") = IFNULL(b. Line_Item_Type ,"") AND
    IFNULL(a. Insertion_Order_ID ,0) = IFNULL(b. Insertion_Order_ID ,0) AND
    IFNULL(a. Insertion_Order_Status ,"") = IFNULL(b. Insertion_Order_Status ,"") AND
    IFNULL(a. Insertion_Order_Goal_Type ,"") = IFNULL(b. Insertion_Order_Goal_Type ,"") AND
    IFNULL(a. Campaign_ID ,0) = IFNULL(b. Campaign_ID ,0) AND
    IFNULL(a. Floodlight_Activity_ID ,0) = IFNULL(b. Floodlight_Activity_ID ,0) AND
    IFNULL(a. Creative_ID ,0) = IFNULL(b. Creative_ID ,0) AND
    IFNULL(a. Creative_Size ,"") = IFNULL(b. Creative_Size ,"") AND
    IFNULL(a. Creative_Status ,"") = IFNULL(b. Creative_Status ,"") AND
    IFNULL(a. Creative_Type ,"") = IFNULL(b. Creative_Type ,"") AND
    IFNULL(a. CM_Placement_ID ,0) = IFNULL(b. CM_Placement_ID ,0)'
sleep 5

sudo bq query --use_legacy_sql=false 'UPDATE `_deepm.'$1'` a
    SET a.Post_Click_Conversions_1D = b.Post_Click_Conversions_1D,
    a.Post_View_Conversions_1D = b.Post_View_Conversions_1D
    FROM `_deepm.'$1'_1D` b
    WHERE
    a. Advertiser_Currency = b. Advertiser_Currency AND
    a.Date=b.Date AND
    IFNULL(a. Partner_ID ,0) = IFNULL(b. Partner_ID ,0) AND
    IFNULL(a. Partner ,"") = IFNULL(b. Partner ,"") AND
    IFNULL(a. Advertiser_ID ,0) = IFNULL(b. Advertiser_ID ,0) AND
    IFNULL(a. Advertiser_Status ,"") = IFNULL(b. Advertiser_Status ,"") AND
    IFNULL(a. Line_Item_ID ,0) = IFNULL(b. Line_Item_ID ,0) AND
    IFNULL(a. Line_Item_Status ,"") = IFNULL(b. Line_Item_Status ,"") AND
    IFNULL(a. Line_Item_Type ,"") = IFNULL(b. Line_Item_Type ,"") AND
    IFNULL(a. Insertion_Order_ID ,0) = IFNULL(b. Insertion_Order_ID ,0) AND
    IFNULL(a. Insertion_Order_Status ,"") = IFNULL(b. Insertion_Order_Status ,"") AND
    IFNULL(a. Insertion_Order_Goal_Type ,"") = IFNULL(b. Insertion_Order_Goal_Type ,"") AND
    IFNULL(a. Campaign_ID ,0) = IFNULL(b. Campaign_ID ,0) AND
    IFNULL(a. Floodlight_Activity_ID ,0) = IFNULL(b. Floodlight_Activity_ID ,0) AND
    IFNULL(a. Creative_ID ,0) = IFNULL(b. Creative_ID ,0) AND
    IFNULL(a. Creative_Size ,"") = IFNULL(b. Creative_Size ,"") AND
    IFNULL(a. Creative_Status ,"") = IFNULL(b. Creative_Status ,"") AND
    IFNULL(a. Creative_Type ,"") = IFNULL(b. Creative_Type ,"") AND
    IFNULL(a. CM_Placement_ID ,0) = IFNULL(b. CM_Placement_ID ,0)'
sleep 5

sudo bq query --use_legacy_sql=false 'UPDATE `_deepm.'$1'` a
    SET a.Post_Click_Conversions_3D = b.Post_Click_Conversions_3D,
    a.Post_View_Conversions_3D = b.Post_View_Conversions_3D
    FROM `_deepm.'$1'_3D` b
    WHERE
    a. Advertiser_Currency = b. Advertiser_Currency AND
    a.Date=b.Date AND
    IFNULL(a. Partner_ID ,0) = IFNULL(b. Partner_ID ,0) AND
    IFNULL(a. Partner ,"") = IFNULL(b. Partner ,"") AND
    IFNULL(a. Advertiser_ID ,0) = IFNULL(b. Advertiser_ID ,0) AND
    IFNULL(a. Advertiser_Status ,"") = IFNULL(b. Advertiser_Status ,"") AND
    IFNULL(a. Line_Item_ID ,0) = IFNULL(b. Line_Item_ID ,0) AND
    IFNULL(a. Line_Item_Status ,"") = IFNULL(b. Line_Item_Status ,"") AND
    IFNULL(a. Line_Item_Type ,"") = IFNULL(b. Line_Item_Type ,"") AND
    IFNULL(a. Insertion_Order_ID ,0) = IFNULL(b. Insertion_Order_ID ,0) AND
    IFNULL(a. Insertion_Order_Status ,"") = IFNULL(b. Insertion_Order_Status ,"") AND
    IFNULL(a. Insertion_Order_Goal_Type ,"") = IFNULL(b. Insertion_Order_Goal_Type ,"") AND
    IFNULL(a. Campaign_ID ,0) = IFNULL(b. Campaign_ID ,0) AND
    IFNULL(a. Floodlight_Activity_ID ,0) = IFNULL(b. Floodlight_Activity_ID ,0) AND
    IFNULL(a. Creative_ID ,0) = IFNULL(b. Creative_ID ,0) AND
    IFNULL(a. Creative_Size ,"") = IFNULL(b. Creative_Size ,"") AND
    IFNULL(a. Creative_Status ,"") = IFNULL(b. Creative_Status ,"") AND
    IFNULL(a. Creative_Type ,"") = IFNULL(b. Creative_Type ,"") AND
    IFNULL(a. CM_Placement_ID ,0) = IFNULL(b. CM_Placement_ID ,0)'
sleep 5

sudo bq query --use_legacy_sql=false 'UPDATE `_deepm.'$1'` a
    SET a.Post_Click_Conversions_7D = b.Post_Click_Conversions_7D,
    a.Post_View_Conversions_7D = b.Post_View_Conversions_7D
    FROM `_deepm.'$1'_7D` b
    WHERE
    a. Advertiser_Currency = b. Advertiser_Currency AND
    a.Date=b.Date AND
    IFNULL(a. Partner_ID ,0) = IFNULL(b. Partner_ID ,0) AND
    IFNULL(a. Partner ,"") = IFNULL(b. Partner ,"") AND
    IFNULL(a. Advertiser_ID ,0) = IFNULL(b. Advertiser_ID ,0) AND
    IFNULL(a. Advertiser_Status ,"") = IFNULL(b. Advertiser_Status ,"") AND
    IFNULL(a. Line_Item_ID ,0) = IFNULL(b. Line_Item_ID ,0) AND
    IFNULL(a. Line_Item_Status ,"") = IFNULL(b. Line_Item_Status ,"") AND
    IFNULL(a. Line_Item_Type ,"") = IFNULL(b. Line_Item_Type ,"") AND
    IFNULL(a. Insertion_Order_ID ,0) = IFNULL(b. Insertion_Order_ID ,0) AND
    IFNULL(a. Insertion_Order_Status ,"") = IFNULL(b. Insertion_Order_Status ,"") AND
    IFNULL(a. Insertion_Order_Goal_Type ,"") = IFNULL(b. Insertion_Order_Goal_Type ,"") AND
    IFNULL(a. Campaign_ID ,0) = IFNULL(b. Campaign_ID ,0) AND
    IFNULL(a. Floodlight_Activity_ID ,0) = IFNULL(b. Floodlight_Activity_ID ,0) AND
    IFNULL(a. Creative_ID ,0) = IFNULL(b. Creative_ID ,0) AND
    IFNULL(a. Creative_Size ,"") = IFNULL(b. Creative_Size ,"") AND
    IFNULL(a. Creative_Status ,"") = IFNULL(b. Creative_Status ,"") AND
    IFNULL(a. Creative_Type ,"") = IFNULL(b. Creative_Type ,"") AND
    IFNULL(a. CM_Placement_ID ,0) = IFNULL(b. CM_Placement_ID ,0)'
sleep 5

sudo python3 lookback.py $1

sudo touch /home/yoptima/DeepVu/advertiser-deeplake/tableau/lock_$1
sleep 5

num=$(ls /home/yoptima/DeepVu/advertiser-deeplake/tableau/lock_* | wc -l)
count=$(crontab -l | grep -e advertiser_load_19D.py -e advertiser_load_19D_without_pixel.py -e deepm_load.py -e deepm_without_name_load.py | wc -l)

if [ $num -eq $count ]
then
    sudo gcloud compute instances stop advertiser-deeplake --zone us-west1-b
fi
