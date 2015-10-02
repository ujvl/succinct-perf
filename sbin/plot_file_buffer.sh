python plot_cdf.py ../results/res_extract 1
python plot_cdf.py ../results/res_count 1
awk '{ print $2/$1 }' ../results/res_search > ../results/res_search_per_occ
python plot_cdf.py ../results/res_search_per_occ 0
