import nori2 
import copy

nori_path = "s3://tf-23q3-shared-data/parsed_data/car_9/20230901/ppl_bag_20230901_161016_det/v0_230906_080437/all_nori/image.nori"
nori_id = "116418929,7010005c1afd31"

with nori2.open(nori_path) as nr:
    f = nr.get(nori_id)
    print(len(f))