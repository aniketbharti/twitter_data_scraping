import os 

for filename in os.listdir(os.path.dirname(os.path.abspath(__file__))):
  base_file, ext = os.path.splitext(filename)
  if not ext == ".pkl":
    os.rename(filename, base_file + ".pkl")