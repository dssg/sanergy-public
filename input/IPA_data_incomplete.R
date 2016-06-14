
require(foreign)
sav_data <- read.spss('data/input/static_box_folder/data/IPA/IPA_data_incomplete.sav')
write.csv(sav_data, 'data/input/IPA_data_incomplete.csv')

