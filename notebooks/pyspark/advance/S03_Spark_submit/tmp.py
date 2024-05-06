fna_parent_path = "hdfs:///dataset/MiDAS_v3/FNA"
fna_file_names = ["pjc", "odd", "dal", "cdt"]

fna_file_paths = {name: f"{fna_parent_path}/{name}.parquet" for name in fna_file_names}

print(fna_file_paths)

