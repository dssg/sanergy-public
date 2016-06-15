require(foreign)

input_file = Sys.getenv("INPUT")
output_file = Sys.getenv("OUTPUT")
sav_data = read.spss(input_file)
write.csv(sav_data, output_file)
