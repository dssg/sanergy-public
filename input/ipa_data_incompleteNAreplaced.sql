drop table if exists input.ipa_data_incomplete;

CREATE TABLE input.ipa_data_incomplete (
	_unnamed INTEGER NOT NULL, 
	hhid INTEGER NOT NULL, 
	date_sta BIGINT NOT NULL, 
	submitda TEXT NOT NULL, 
	starttim TEXT NOT NULL, 
	date_end BIGINT NOT NULL, 
	endtime TEXT NOT NULL, 
	deviceid FLOAT NOT NULL, 
	subscrib FLOAT NOT NULL, 
	simid FLOAT NOT NULL, 
	consent BOOLEAN NOT NULL, 
	time BIGINT NOT NULL, 
	language TEXT NOT NULL, 
	toiletnu TEXT NOT NULL, 
	price INTEGER NOT NULL, 
	consent_ BOOLEAN NOT NULL, 
	a1_perso TEXT NOT NULL, 
	a3i_agey INTEGER NOT NULL, 
	a3ii_age TEXT, 
	a4_gend0 TEXT NOT NULL, 
	a5_relig TEXT NOT NULL, 
	a5_reli0 TEXT NOT NULL, 
	a6_tribe TEXT NOT NULL, 
	a6_trib0 TEXT NOT NULL, 
	a7_relat TEXT NOT NULL, 
	a7_rela0 TEXT NOT NULL, 
	a8_marit TEXT NOT NULL, 
	a8_mari0 TEXT NOT NULL, 
	a9_occup BOOLEAN NOT NULL, 
	a10_pryo TEXT NOT NULL, 
	a10_pry0 TEXT NOT NULL, 
	a11_enga TEXT NOT NULL, 
	a12_earn FLOAT NOT NULL, 
	a13_educ TEXT NOT NULL, 
	a1_pers0 TEXT NOT NULL, 
	a3i_age0 INTEGER NOT NULL, 
	a3ii_ag0 TEXT, 
	a5_reli1 TEXT NOT NULL, 
	a5_reli2 TEXT NOT NULL, 
	a6_trib1 TEXT NOT NULL, 
	a6_trib2 TEXT NOT NULL, 
	a7_rela1 TEXT NOT NULL, 
	a7_rela2 TEXT NOT NULL, 
	a8_mari1 TEXT NOT NULL, 
	a8_mari2 TEXT NOT NULL, 
	a9_occu0 BOOLEAN NOT NULL, 
	a10_pry1 TEXT NOT NULL, 
	a10_pry2 TEXT NOT NULL, 
	a11_eng0 TEXT NOT NULL, 
	a12_ear0 INTEGER NOT NULL, 
	a13_edu0 TEXT NOT NULL, 
	a1_pers1 TEXT, 
	a3i_age1 TEXT, 
	a3ii_ag1 TEXT, 
	a5_reli3 TEXT, 
	a5_reli4 TEXT NOT NULL, 
	a6_trib3 TEXT, 
	a6_trib4 TEXT NOT NULL, 
	a7_rela3 TEXT, 
	a7_rela4 TEXT NOT NULL, 
	a8_mari3 TEXT, 
	a8_mari4 TEXT NOT NULL, 
	a9_occu1 TEXT, 
	a10_pry3 TEXT, 
	a10_pry4 TEXT NOT NULL, 
	a11_eng1 TEXT, 
	a12_ear1 TEXT, 
	a4_gend1 TEXT, 
	a13_edu1 TEXT, 
	ppi_2_total TEXT, 
	a1_pers2 TEXT, 
	a3i_age2 TEXT, 
	a3ii_ag2 TEXT, 
	a4_gend2 TEXT, 
	a5_reli5 TEXT, 
	a5_reli6 TEXT NOT NULL, 
	a6_trib5 TEXT, 
	a6_trib6 TEXT NOT NULL, 
	a7_rela5 TEXT, 
	a7_rela6 TEXT NOT NULL, 
	a8_mari5 TEXT, 
	a8_mari6 TEXT NOT NULL, 
	a9_occu2 TEXT, 
	a10_pry5 TEXT, 
	a10_pry6 TEXT NOT NULL, 
	a11_eng2 TEXT, 
	a12_ear2 TEXT, 
	a13_edu2 TEXT, 
	a1_pers3 TEXT, 
	a3i_age3 TEXT, 
	a3ii_ag3 TEXT, 
	a4_gend3 TEXT, 
	a5_reli7 TEXT, 
	a5_reli8 TEXT NOT NULL, 
	a6_trib7 TEXT, 
	a6_trib8 TEXT NOT NULL, 
	a7_rela7 TEXT, 
	a7_rela8 TEXT NOT NULL, 
	a8_mari7 TEXT, 
	a8_mari8 TEXT NOT NULL, 
	a9_occu3 TEXT, 
	a10_pry7 TEXT, 
	a10_pry8 TEXT NOT NULL, 
	a11_eng3 TEXT, 
	a12_ear3 TEXT, 
	a13_edu3 TEXT, 
	a1_pers4 TEXT, 
	a3i_age4 TEXT, 
	a3ii_ag4 TEXT, 
	a4_gend4 TEXT, 
	a5_reli9 TEXT, 
	a5_rel00 TEXT NOT NULL, 
	a6_trib9 TEXT, 
	a6_tri00 TEXT NOT NULL, 
	a7_rela9 TEXT, 
	a7_rel00 TEXT NOT NULL, 
	a8_mari9 TEXT, 
	a8_mar00 TEXT NOT NULL, 
	a9_occu4 TEXT, 
	a10_pry9 TEXT, 
	a10_pr00 TEXT NOT NULL, 
	a11_eng4 TEXT, 
	a12_ear4 TEXT, 
	a13_edu4 TEXT, 
	a1_pers5 TEXT, 
	a3i_age5 TEXT, 
	a3ii_ag5 TEXT, 
	a4_gend5 TEXT, 
	a5_rel01 TEXT, 
	a5_rel02 TEXT NOT NULL, 
	a6_tri01 TEXT, 
	a6_tri02 TEXT NOT NULL, 
	a7_rel01 TEXT, 
	a7_rel02 TEXT NOT NULL, 
	a8_mar01 TEXT, 
	a8_mar02 TEXT NOT NULL, 
	a9_occu5 TEXT, 
	a10_pr01 TEXT, 
	a10_pr02 TEXT NOT NULL, 
	a11_eng5 TEXT, 
	a12_ear5 TEXT, 
	a13_edu5 TEXT, 
	a1_pers6 TEXT, 
	a3i_age6 TEXT, 
	a3ii_ag6 TEXT, 
	a4_gend6 TEXT, 
	a5_rel03 TEXT, 
	a5_rel04 TEXT NOT NULL, 
	a6_tri03 TEXT, 
	a6_tri04 TEXT NOT NULL, 
	a7_rel03 TEXT, 
	a7_rel04 TEXT NOT NULL, 
	a8_mar03 TEXT, 
	a8_mar04 TEXT NOT NULL, 
	a9_occu6 TEXT, 
	a10_pr03 TEXT, 
	a10_pr04 TEXT NOT NULL, 
	a11_eng6 TEXT, 
	a12_ear6 TEXT, 
	a13_edu6 TEXT, 
	a1_pers7 TEXT, 
	a3i_age7 TEXT, 
	a3ii_ag7 TEXT, 
	a4_gend7 TEXT, 
	a5_rel05 TEXT, 
	a5_rel06 TEXT NOT NULL, 
	a6_tri05 TEXT, 
	a6_tri06 TEXT NOT NULL, 
	a7_rel05 TEXT, 
	a7_rel06 TEXT NOT NULL, 
	a8_mar05 TEXT, 
	a8_mar06 TEXT NOT NULL, 
	a9_occu7 TEXT, 
	a10_pr05 TEXT, 
	a10_pr06 TEXT NOT NULL, 
	a11_eng7 TEXT, 
	a12_ear7 TEXT, 
	a13_edu7 TEXT, 
	a1_pers8 TEXT, 
	a2_perso TEXT NOT NULL, 
	a3i_age8 TEXT, 
	a3ii_ag8 TEXT, 
	a4_gend8 TEXT, 
	a5_rel07 TEXT, 
	a5_rel08 TEXT NOT NULL, 
	a6_tri07 TEXT, 
	a6_tri08 TEXT NOT NULL, 
	a7_rel07 TEXT, 
	a7_rel08 TEXT NOT NULL, 
	a8_mar07 TEXT, 
	a8_mar08 TEXT NOT NULL, 
	a9_occu8 TEXT, 
	a10_pr07 TEXT, 
	a10_pr08 TEXT NOT NULL, 
	a11_eng8 TEXT, 
	a12_ear8 TEXT, 
	a13_edu8 TEXT, 
	a19_phon BOOLEAN NOT NULL, 
	a20_whos TEXT, 
	a20_who0 TEXT NOT NULL, 
	a20_who_ TEXT NOT NULL, 
	a21_phon TEXT, 
	a22_whos TEXT, 
	a22_who_ TEXT NOT NULL, 
	a22_who0 TEXT NOT NULL, 
	a23_born BOOLEAN NOT NULL, 
	a24_live TEXT NOT NULL, 
	a24i_rur TEXT NOT NULL, 
	a24i_ru0 TEXT NOT NULL, 
	a24ii_to TEXT, 
	a24ii_nr TEXT NOT NULL, 
	a24ii_ot TEXT NOT NULL, 
	a25_live INTEGER NOT NULL, 
	a26_live INTEGER NOT NULL, 
	b1_chang BOOLEAN NOT NULL, 
	b2_situa TEXT NOT NULL, 
	b3_ladde INTEGER NOT NULL, 
	b4_1 TEXT NOT NULL, 
	b4_2 TEXT NOT NULL, 
	b4_3 TEXT NOT NULL, 
	b4_4 TEXT NOT NULL, 
	b4_5 TEXT NOT NULL, 
	b5_1 TEXT NOT NULL, 
	b5_2 TEXT NOT NULL, 
	b5_3 TEXT NOT NULL, 
	b5_4 TEXT NOT NULL, 
	b5_5 TEXT NOT NULL, 
	b6_sad INTEGER NOT NULL, 
	b7_worri INTEGER NOT NULL, 
	b8_happy INTEGER NOT NULL, 
	c1_takeo BOOLEAN NOT NULL, 
	c2_takeo BOOLEAN NOT NULL, 
	c3_takes BOOLEAN NOT NULL, 
	c4_takes BOOLEAN NOT NULL, 
	c5_takeh BOOLEAN NOT NULL, 
	c6_healt TEXT, 
	c7_takeh BOOLEAN NOT NULL, 
	c8_probl TEXT, 
	c9_medic BOOLEAN NOT NULL, 
	c10_prob TEXT NOT NULL, 
	c11_tabl BOOLEAN NOT NULL, 
	c12_heal TEXT NOT NULL, 
	c13_suff BOOLEAN NOT NULL, 
	c14_days TEXT, 
	c15_diar BOOLEAN NOT NULL, 
	c16_days TEXT, 
	d1_bever TEXT, 
	tea INTEGER NOT NULL, 
	coffee TEXT, 
	soda TEXT, 
	juice INTEGER NOT NULL, 
	soup TEXT, 
	porridge INTEGER NOT NULL, 
	v115 TEXT, 
	d1_beve0 TEXT NOT NULL, 
	d2_rank TEXT NOT NULL, 
	d3_rank2 TEXT NOT NULL, 
	d4_free2 BOOLEAN NOT NULL, 
	d5d5_opt TEXT, 
	d5d5_com INTEGER NOT NULL, 
	d5d5_co0 INTEGER NOT NULL, 
	d5d5_pri TEXT, 
	d5d5_flp TEXT, 
	d5d5_non TEXT, 
	d5d5_nei TEXT, 
	d5d5_fre TEXT, 
	d5d5_toi TEXT, 
	d5d5_to0 TEXT, 
	d5d5_ope TEXT, 
	d5d5_fly TEXT, 
	d5d5_oth TEXT, 
	d5_optio TEXT NOT NULL, 
	d6_group TEXT, 
	d6_grou0 TEXT, 
	d6_grou1 TEXT, 
	d6_grou2 TEXT, 
	d6_grou3 TEXT, 
	d6_grou4 TEXT, 
	d6_grou5 TEXT, 
	d6_grou6 INTEGER NOT NULL, 
	d6_optin TEXT NOT NULL, 
	d7_high_ TEXT NOT NULL, 
	d8_rankn TEXT NOT NULL, 
	d9_free BOOLEAN NOT NULL, 
	d10_scal TEXT, 
	d11_frie TEXT NOT NULL, 
	d12_impo TEXT, 
	d13_rank TEXT, 
	d14_rank TEXT, 
	e1_group TEXT, 
	e1_grou0 TEXT, 
	e1_grou1 INTEGER NOT NULL, 
	e1_grou2 INTEGER NOT NULL, 
	e1_grou3 TEXT, 
	e1_grou4 TEXT, 
	e1_grou5 TEXT, 
	e1_consi TEXT, 
	e2_1 TEXT NOT NULL, 
	e2_2 TEXT NOT NULL, 
	e2_3 TEXT, 
	e2_4 TEXT, 
	e2_5 TEXT, 
	e2_6 TEXT, 
	e2_7 TEXT, 
	e2_8 TEXT, 
	e2_9 TEXT, 
	e2_10 TEXT, 
	e2_11 TEXT, 
	e2_12 TEXT, 
	e2_other TEXT NOT NULL, 
	toilet_o INTEGER NOT NULL, 
	toilet_0 INTEGER NOT NULL, 
	e2_optio TEXT NOT NULL, 
	toilete3 INTEGER NOT NULL, 
	toilete0 INTEGER NOT NULL, 
	toilete4 INTEGER NOT NULL, 
	toilete5 INTEGER NOT NULL, 
	toilete6 BOOLEAN NOT NULL, 
	toilete7 BOOLEAN NOT NULL, 
	toilete8 BOOLEAN NOT NULL, 
	toilet_1 INTEGER NOT NULL, 
	e2_opti0 TEXT NOT NULL, 
	toilete1 INTEGER NOT NULL, 
	toilete2 INTEGER NOT NULL, 
	toilete9 INTEGER NOT NULL, 
	toilet00 INTEGER NOT NULL, 
	toilet01 BOOLEAN NOT NULL, 
	toilet02 BOOLEAN NOT NULL, 
	toilet03 BOOLEAN NOT NULL, 
	toilet_2 INTEGER NOT NULL, 
	e2_opti1 TEXT NOT NULL, 
	toilet04 TEXT, 
	toilet05 TEXT, 
	toilet06 TEXT, 
	toilet07 TEXT, 
	toilet08 TEXT, 
	toilet09 TEXT, 
	toilet10 TEXT, 
	toilet_3 INTEGER NOT NULL, 
	e2_opti2 TEXT NOT NULL, 
	toilet11 TEXT, 
	toilet12 TEXT, 
	toilet13 TEXT, 
	toilet14 TEXT, 
	toilet15 TEXT, 
	toilet16 TEXT, 
	toilet17 TEXT, 
	toilet_4 INTEGER NOT NULL, 
	e2_opti3 TEXT NOT NULL, 
	toilet18 TEXT, 
	toilet19 TEXT, 
	toilet20 TEXT, 
	toilet21 TEXT, 
	toilet22 TEXT, 
	toilet23 TEXT, 
	toilet24 TEXT, 
	toilet_5 INTEGER NOT NULL, 
	e2_opti4 TEXT NOT NULL, 
	toilet25 TEXT, 
	toilet26 TEXT, 
	toilet27 TEXT, 
	toilet28 TEXT, 
	toilet29 TEXT, 
	toilet30 TEXT, 
	toilet31 TEXT, 
	toilet_6 TEXT, 
	e2_opti5 TEXT, 
	toilet32 TEXT, 
	toilet33 TEXT, 
	toilet34 TEXT, 
	toilet35 TEXT, 
	toilet36 TEXT, 
	toilet37 TEXT, 
	toilet38 TEXT, 
	toilet_7 TEXT, 
	e2_opti6 TEXT, 
	toilet39 TEXT, 
	toilet40 TEXT, 
	toilet41 TEXT, 
	toilet42 TEXT, 
	toilet43 TEXT, 
	toilet44 TEXT, 
	toilet45 TEXT, 
	toilet_8 TEXT, 
	e2_opti7 TEXT, 
	toilet46 TEXT, 
	toilet47 TEXT, 
	toilet48 TEXT, 
	toilet49 TEXT, 
	toilet50 TEXT, 
	toilet51 TEXT, 
	toilet52 TEXT, 
	toilet_9 TEXT, 
	e2_opti8 TEXT, 
	toilet53 TEXT, 
	toilet54 TEXT, 
	toilet55 TEXT, 
	toilet56 TEXT, 
	toilet57 TEXT, 
	toilet58 TEXT, 
	toilet59 TEXT, 
	toilet60 TEXT, 
	e2_opti9 TEXT, 
	toilet61 TEXT, 
	toilet62 TEXT, 
	toilet63 TEXT, 
	toilet64 TEXT, 
	toilet65 TEXT, 
	toilet66 TEXT, 
	toilet67 TEXT, 
	toilet68 TEXT, 
	e2_opt00 TEXT, 
	toilet69 TEXT, 
	toilet70 TEXT, 
	toilet71 TEXT, 
	toilet72 TEXT, 
	toilet73 TEXT, 
	toilet74 TEXT, 
	toilet75 TEXT, 
	e9_wash TEXT, 
	e10_wash TEXT, 
	e11_circ TEXT, 
	e11_cir0 TEXT, 
	e12_reas TEXT, 
	e12_rea0 TEXT, 
	e12_rea1 TEXT, 
	e12_rea2 TEXT, 
	e12_rea3 TEXT, 
	e12_rea4 TEXT, 
	e12_rea5 TEXT, 
	e13_inop TEXT, 
	e14_open TEXT, 
	e15_circ TEXT, 
	e15_cir0 TEXT, 
	e16_reas TEXT, 
	e16_rea0 TEXT, 
	e16_rea1 TEXT, 
	e16_rea2 TEXT, 
	e16_rea3 TEXT, 
	e16_rea4 TEXT, 
	e16_rea5 TEXT, 
	e16_rea6 TEXT, 
	e17_forc TEXT, 
	e18_forc TEXT, 
	e19_used TEXT, 
	e20_know TEXT, 
	e20_kno0 TEXT, 
	e20_kno1 TEXT, 
	e20_kno2 TEXT, 
	e20_kno3 TEXT, 
	e20_kno4 TEXT, 
	e21_flt_ TEXT, 
	e21_dont TEXT, 
	e22_ofte TEXT, 
	e22_oft0 TEXT, 
	e23_usef TEXT, 
	e23_use0 TEXT, 
	e23_use1 TEXT, 
	e23_use2 TEXT, 
	e24_purp TEXT, 
	e25_grou TEXT, 
	e25_gro0 TEXT, 
	e25_gro1 TEXT, 
	e25_gro2 TEXT, 
	e25_gro3 TEXT, 
	e25_gro4 TEXT, 
	e25_gro5 TEXT, 
	e25_gro6 TEXT, 
	e25_opti TEXT, 
	e26_know TEXT, 
	e27_rshi TEXT, 
	e27_rsh0 TEXT, 
	e28_clos TEXT, 
	e29_grou TEXT, 
	e29_gro0 TEXT, 
	e29_gro1 TEXT, 
	e29_gro2 TEXT, 
	e29_gro3 TEXT, 
	e29_gro4 TEXT, 
	e29_gro5 TEXT, 
	e29_gro6 TEXT, 
	e29_opti TEXT, 
	e30_deci TEXT, 
	e30_dec0 TEXT, 
	e31_usef TEXT, 
	satisfie TEXT, 
	e32_dist TEXT, 
	e33_clea TEXT, 
	e34_pric TEXT, 
	e35_frie TEXT, 
	e36_priv TEXT, 
	e37_safe TEXT, 
	e38_open TEXT, 
	e39_wate TEXT, 
	e40_soap TEXT, 
	e41_toil TEXT, 
	e42e42_o TEXT, 
	e42e42_c TEXT, 
	e42e42_t TEXT, 
	e42e42_p TEXT, 
	e42e42_w TEXT, 
	e42e42_s TEXT, 
	e42e42_0 TEXT, 
	e42e42_f TEXT, 
	e42e42_1 TEXT, 
	e42e42_e TEXT, 
	e42e42_l TEXT, 
	e42e42_2 TEXT, 
	e42e42_3 TEXT, 
	e42_impr TEXT, 
	e43_know TEXT, 
	e44_toil TEXT, 
	e44_awar TEXT, 
	e45_know TEXT, 
	e45_kno0 TEXT, 
	e46_grou TEXT, 
	e46_gro0 TEXT, 
	e46_gro1 TEXT, 
	e46_gro2 TEXT, 
	e46_gro3 TEXT, 
	e46_gro4 TEXT, 
	e46_gro5 TEXT, 
	e46_gro6 TEXT, 
	e46_notu TEXT, 
	e47_hhus TEXT, 
	e48_know TEXT, 
	e49_know TEXT, 
	e50e50_o TEXT, 
	e50e50_c TEXT, 
	e50e50_t TEXT, 
	e50e50_p TEXT, 
	e50e50_w TEXT, 
	e50e50_s TEXT, 
	e50e50_0 TEXT, 
	e50e50_f TEXT, 
	e50e50_1 TEXT, 
	e50e50_e TEXT, 
	e50e50_l TEXT, 
	e50e50_2 TEXT, 
	e50e50_3 TEXT, 
	e50_impr TEXT, 
	f1_busin TEXT, 
	f1_busi0 TEXT, 
	f2_room TEXT, 
	f3_mater TEXT, 
	f4_light TEXT, 
	f5_irons TEXT, 
	f6_nets TEXT, 
	f7_towel TEXT, 
	f8_pan TEXT, 
	window1 TEXT, 
	window2 TEXT, 
	envelopn TEXT, 
	"group" TEXT, 
	length TEXT, 
	marketin TEXT, 
	discount TEXT, 
	hh_gps_a TEXT, 
	consente TEXT, 
	metainst TEXT, 
	key TEXT, 
	responde TEXT, 
	v109 TEXT, 
	v110 TEXT, 
	v111 TEXT, 
	v112 TEXT, 
	v113 TEXT, 
	v114 TEXT, 
	incomple TEXT, 
	numcompl TEXT, 
	missingg TEXT, 
	intervie TEXT, 
	response TEXT, 
	tag TEXT, 
	d5d5_most_common_toilet TEXT, 
	d5d5_most_common_toilet_2 TEXT, 
	d5d5_most_common_toilet_3 TEXT, 
	d6_reason_for_use_1 TEXT, 
	d6_reason_for_use_2 TEXT, 
	d6_reason_for_use_3 TEXT, 
	e1_think_before_use_1 TEXT, 
	e1_think_before_use_2 TEXT, 
	e1_think_before_use_3 TEXT, 
	e_20_marketing_channels_1 TEXT, 
	e_20_marketing_channels_2 TEXT, 
	e_20_marketing_channels_3 TEXT, 
	e_20_marketing_channels_4 TEXT, 
	e_20_marketing_channels_5 TEXT, 
	e_20_marketing_channels_total TEXT, 
	e25_reason_for_use_1 TEXT, 
	e25_reason_for_use_2 TEXT, 
	e25_reason_for_use_3 TEXT, 
	a1_hh_members_total TEXT, 
	ppi_1 TEXT, 
	ppi_3 TEXT, 
	ppi_4 TEXT, 
	ppi_5 TEXT, 
	ppi_6 TEXT, 
	ppi_7 TEXT, 
	ppi_8 TEXT, 
	ppi_9 TEXT, 
	ppi_10 TEXT, 
	a4_gende TEXT, 
	a4_hh TEXT, 
	a4_hh_0 TEXT, 
	a4_hh_1 TEXT, 
	a4_hh_2 TEXT, 
	a4_hh_3 TEXT, 
	a4_hh_4 TEXT, 
	a4_hh_5 TEXT, 
	a4_hh_6 TEXT, 
	a4_hh_7 TEXT, 
	a4_hh_8 TEXT, 
	a4_gende_recode TEXT, 
	a4_gend0_recode TEXT, 
	a4_gend1_recode TEXT, 
	a4_gend2_recode TEXT, 
	a4_gend3_recode TEXT, 
	a4_gend4_recode TEXT, 
	a4_gend5_recode TEXT, 
	a4_gend6_recode TEXT, 
	a4_gend7_recode TEXT, 
	a4_gend8_recode TEXT, 
	a4_hh_females TEXT, 
	"filter_." TEXT, 
	a4_hh_members_minus_females TEXT, 
	ppi_2 TEXT, 
	ppi_total TEXT, 
	ppi_total_bands TEXT, 
	"ppi_total_1.25" TEXT, 
	"ppi_total_2.5" TEXT, 
	ppi_total_4 TEXT, 
	"ppi_total_8.44" TEXT, 
	d5d5_fli_nonfli TEXT
);

\copy input.ipa_data_incomplete from 'data/input/IPA_data_incomplete.csv' with csv header;