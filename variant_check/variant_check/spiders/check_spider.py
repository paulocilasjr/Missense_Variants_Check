import scrapy
import json
import pandas as pd
import urllib.parse

def isDone (current, limit):
    if current < limit:
        return False
    elif current == limit or current > limit:
        return True

def validResult (result):
    if result > 0:
        return True
    else:
        return False

def validSearch (term):
    if term == "":
        return False
    else: 
        True

def BuildNextPage (term):
    term = urllib.parse.quote(term)
    return f"https://brcaexchange.org/backend/data/?format=json&order_by=Gene_Symbol&direction=ascending&page_size=1&page_num=0&search_term={term}&include=Variant_in_ENIGMA&include=Variant_in_ClinVar&include=Variant_in_1000_Genomes&include=Variant_in_ExAC&include=Variant_in_LOVD&include=Variant_in_BIC&include=Variant_in_ESP&include=Variant_in_exLOVD&include=Variant_in_ENIGMA_BRCA12_Functional_Assays&include=Variant_in_GnomAD&include=Variant_in_GnomADv3"

class CheckSpider(scrapy.Spider):
    name = "check_variants"
    list_variants = ["c.813G>A p.D232N",
"c.1519A>G p.K467R",
"c.1806C>T p.Q563X",
"442-?_593+?del",
"BRCA1",
"BRCA1",
"BRCA1",
"BRCA1",
"BRCA1",
"BRCA1 mutation-no report",
"BRCA1,NPEPPS",
"BRCA1,PLEKHA7",
"BRCA1,TRAF3",
"c.2836G>C",
"c.499G>A p.Gly167Arg",
"c.7592_7595delITTCCins10 p.Val2531_Pro2532delinsAVGG",
"c.8850G>T p.Lys2950Asn",
"NM_007294.2",
"p.1835",
"p.E1134",
"p.M1173V",
"p.N3124l",
"p.Q1273",
"p.Q1538",
"Variant Not Stated",
"Variant Not Stated",
"187delAG",
"c.187delAG",
"c.181T>G c.181T>G (p.Cys61Gly)",
"c.18IT>G p.Cys61Gly",
"c.IVF5-1 G>A",
"c.300T>G p.C81G",
"c.c1061A>G",
"c.c1418A>T p.Asn47lle",
"c.c1703C>T p.P568L",
"c.2154A>T p.K6789X",
"c.c2246A>T p.Asp749Val",
"BRCA1 N1016fs",
"4184DEL4",
"c.4964_4982del19-report",
"IVS 18-2delA",
"p.K1727*",
"c.5266dupC p.Gln1758Profs*74",
"BRCA1 G1803A",
"IVS23-18T>A",
"c.IVS8+8T>G",
"c.IVS1-17_IVS1-13delTTTCTInsAA",
"c.IVS1-17_IVS1-13delTTTCTinsAA",
"c.IVS10-11T>G",
"c.IVS16+6T>C",
"c.26delC (p.Pro9GlnfsX16)",
"c.66dup p.Glu23Argfs*18",
"c.68_69del p.Glu23Valfs*17",
"c.68_69delAG",
"p.Glu23Valfs*17",
"BRCA1 c.68_69delAG p.E23fs*17",
"BRCA1 c.90G>C p.L30F",
"BRCA1 p.K38N",
"c.115T>C p.Cys39Arg",
"p.C39R",
"c.128T>C",
"c.135-1G>T",
"p.V47F",
"BRCA1 p.C61G",
"BRCA1 c.181T>G p.Cys61Gly",
"c.181T>G p.Cys61Gly",
"p.C61G",
"p.Cys61Gly",
"BRCA1 c.190T>G p.C64G",
"c.202A>G (p.lle68VAL)",
"c.212+1G>A",
"BRCA1 c.213-11T>G Intronic",
"c.213-12A>G",
"c.213-12A>G",
"c.230C>T p.Thr77Met",
"p.T77M",
"c.300T>G p.C61G",
"BRCA1 c.302-2A>T",
"p.A102G",
"BRCA1 c.329dupA E111fs",
"c.347A>G p.E116G",
"BRCA1 c.351T>A p.H117Q",
"BRCA1 c.396C>A p.N132K",
"BRCA1 c.413T>C p.L138P",
"BRCA1 c.427G>T p.Glu143*",
"BRCA1 c.514delC p.Q172fs*62",
"BRCA1 Q172fs",
"c.515A>G p.Gln172Arg",
"c.525insA",
"BRCA1 c.553G>A p.D185N",
"BRCA1 p.N192S",
"BRCA1 c.601G>C p.D201H",
"BRCA1 p.Q202*",
"BRCA1 p.T231=",
"c.697_698delGT p.Val233AsnfsX4",
"BRCA1 c.729T>G p.N243K",
"BRCA1 c.736T>G p.L246V",
"BRCA1 c.738G>C p.L246F",
"c.798_799delTT p.Ser267Lysfs*19",
"c.811G>C p.V271L",
"BRCA1 c.815_824dup p.Thr276Alafs*14",
"c.815_824dup10",
"c.815_824dupAGCCATGTGG p.Thr276Alafs*14",
"BRCA1 p.R296I",
"BRCA1 c.914_915delGT p.C305fs*1",
"BRCA1 c.946A>G p.Ser316Gly",
"BRCA1 c.962G>A p.Trp321*",
"BRCA1 c.962G>A p.W321*",
"BRCA1 c.976G>C p.E326Q",
"c.1016dupA p.Val340GlyfsX6",
"BRCA1 c.1030G>A p.Ala344Thr",
"p.Asn354Ser",
"c.1082_1092del p.Ser361",
"p.Tyr422His",
"BRCA1 p.I441V",
"c.1333G>C p.E445Q",
"BRCA1 p.E453K",
"c.1360_1361del p.Ser454*",
"c.1400A>G p.Lys467Arg",
"BRCA1 c.1418A>T p.N473I",
"c.1511G>A p.Arg504His",
"p.L512F",
"BRCA1 p.A521T",
"p.L523V",
"c.1568T>G p.Leu523Trp",
"c.1581G>C p.Lys527Asn",
"BRCA1 c.1674del p.Gly559Valfs*13",
"c.1675delA",
"c.1687C>T p.Gln563",
"c.1687C>T p.Gln563Ter",
"c.1687C>T p.Q563*",
"p.Q563*",
"BRCA1 p.E575G",
"BRCA1 E597K",
"c.1820A>G p.Lys607Arg",
"BRCA1 c.1836_1837insG p.R613fs*12",
"BRCA1 c.1846_1848delTCT p.S616del",
"c.1875del p.Val626*",
"c.1881C>G",
"c.1881C>G",
"c.1899A>G p.M594V",
"c.1941T>G p.Ser647Arg",
"BRCA1 p.K654fs",
"c.1961delA",
"c.1996C>A p.L666l",
"c.2071delA p.Arg691AspfsTer10",
"BRCA1 c.2101A>T p.K701*",
"p.K719E",
"BRCA1 c.2155A>G p.K719E",
"c.2199del p.Lys734Asnfs*2",
"BRCA1 c.2222C>G p.S741C",
"BRCA1 c.2289_2697del409 p.V764fs*100",
"c.2359dupG c.2359dupG (p.Glu787Glyfs 3)",
"c.2411_2412del c.2411_2412del (p.Gln804Leufs*5)",
"c.2411_2412delAG",
"c.2457del p.Asp821llefs*25",
"c.2475delC",
"c.2518A>T p.S840C",
"BRCA1 c.2635G>T p.E879*",
"BRCA1 c.2657C>G p.S886C",
"BRCA1 c.2657C>G p.S886C",
"BRCA1 c.2673_2697del25 p.K893fs*99",
"BRCA1 c.2683_2684del p.Q895fs",
"BRCA1 c.2744_2745delCT p.S915fs*1",
"c.2771A>T p.N924l",
"c.2845G>A p.Gly949Ser",
"c.2884G>A p.E962K",
"c.2892A>C p.I925L",
"BRCA1 c.2963C>T p.S988L",
"BRCA1 c.2974A>T T992S",
"p.L999R",
"BRCA1 c.2999A>G p.Glu1000Gly",
"BRCA1 c.3022A>G p.M1008V",
"BRCA1 p.E1013*",
"BRCA1 c.3083G>T p.R1028L",
"p.Asn1029Argfs*5",
"c.3181A>G. p.Ile1061Val",
"BRCA1 c.3193dup p.Asp1065Glyfs*2",
"c.3247A>C p.Met1083Leu",
"BRCA1 c.3274G>C p.E1092Q",
"c.3275A>G p.Glu1092Gly",
"c.3289A>T p.S1097C",
"BRCA1 c.3296C>T p.P1099L",
"BRCA1 c.3296delC p.P1099fs*10",
"c.3305A>G p.Asn1102Ser",
"c.3367G>T p.Asp1123Tyr",
"BRCA1 p.E1134X",
"BRCA1 c.3403delC p.Q1135fs*20",
"BRCA1 p.E1148Q",
"c.3481_3491del11 p.Glu1161PhefsX3",
"BRCA1 S1173I",
"p.V1181l",
"BRCA1 c.3556C>G p.L1186V",
"c.3573G>A p.D1152N",
"c.3587C>T p.Thr1196lle",
"BRCA1 c.3624_3639delATTAGAGTCCTCAGAA p.L1209fs*21",
"BRCA1 E1210fs",
"p.S1217P",
"BRCA1 c.3653G>A p.S1218N",
"c.3661G>T p.Glu1221*",
"c.3700_3704del p.Val1234Glnfg=g",
"c.3700_3704del p.Val1234Glnfs*g",
"c.3700_3704delGTAAA",
"BRCA1 c.3700_3704delGTAAA p.V1234fs*8",
"BRCA1 c.3700_3721>CAATACTATG p.V1234_S1241>QYYA",
"c.3748G>T p.Glu1250*",
"c.3756_3759del p.Ser1253Argfs*10",
"BRCA1 c.3756_3759delGTCT p.S1253fs*10",
"BRCA1 c.3762G>C p.K1254N",
"c.3764dupA p.Asn1255Lysfs*12",
"c.3783G>T p.E1222X",
"BRCA1 c.3800T>C p.L1267S",
"BRCA1 c.3835G>A p.Ala1279Thr",
"c.3857G>T p.Ser1286lle",
"BRCA1 p.L1340F",
"BRCA1 c.4036G>A p.E1346K",
"BRCA1 c.4036G>C p.E1346Q",
"BRCA1 c.4040G>C p.R1347T",
"BRCA1 c.4054G>A p.E1352K",
"BRCA1 c.4065_4068delTCAA N1355fs",
"BRCA1 c.4065_4068delTCAA p.N1355fs*10",
"c.4082T>C p.Met1361Thr",
"c.4096+1G>A",
"BRCA1 c.4115G>A p.C1372Y",
"c.4129A>G p.S1377G",
"BRCA1 c.4132G>A V1378I",
"c.4165_4166del p.Ser1389",
"p.Q1395*",
"BRCA1 c.4190G>A p.R1397K",
"BRCA1 p.R1397K",
"BRCA1 E1419Q",
"BRCA1 c.4258C>T p.Q1420*",
"BRCA1 c.4262A>G p.H1421R",
"c.4327C>T p.Arg1443*",
"c.4327C>T p.Arg1443Ter",
"c.4338A>T p.Glu1446Asp",
"c.4339C>A p.Q1447k",
"c.4357+1G>A",
"BRCA1 c.4397G>A p.S1466N",
"BRCA1 c.4487C>T p.S1496L",
"c.4520G>C p.Arg1507Thr",
"BRCA1 p.D1546E",
"c.4643C>T c.4643C>T(p.Thr1548Met)",
"c.4649C>T p.Thr1550lle",
"p.T1550I",
"c.4674A>G",
"BRCA1 E1559K",
"c.4689C>G p.Tyr1563Ter",
"BRCA1 c.4765C>T p.R1589C",
"BRCA1 c.4780C>G p.Pro1594Ala",
"c.4868C>G p.Ala1623Gly",
"BRCA1 p.P1637S",
"c.4936delG",
"BRCA1 p.M1652V",
"c.4964_4982del p.Ser1655Tyrfs*16",
"c.4964_4982del19",
"c.4986+6T>C",
"BRCA1 c.4987-13G>A",
"BRCA1 c.5005G>T p.A1669S",
"c.5023A>T c.5023A>T (p.T1675S",
"p.T1675S",
"BRCA1 c.5036T>C p.Leu1679Pro",
"c.5075-1G>A",
"c.5096G>A p.Arg1699Gin",
"c.5110T>C p.Phe1704Leu",
"c.5123C>T p.A1708G",
"BRCA1 c.5136G>A p.W1712*",
"BRCA1 p.W1718C",
"c.5177_5180del p.Arg1726Lysfs*3",
"c.5177_5180delGAAA p.R1726Kfs*3",
"c.5179A>T",
"c.5179A>T p.Lys1727",
"c.5179A>T p.Lys1727*",
"BRCA1 c.5191G>T p.E1731*",
"c.5215G>A p.R1699Q",
"BRCA1 p.G1743A",
"BRCA1 p.R1751L",
"c.5266dup p.Gln1756Profs*74",
"c.5266dupC",
"c.5266dupC",
"c.5266dupC p.Gin1756Profs*74",
"c.5266dupC p.Gln1756Profs*74",
"c.5266dupC p.Q1756Pfs*74",
"BRCA1 c.5266_5267insC p.Q1756fs*74",
"BRCA1 c.5266dup p.Q1756fs",
"BRCA1 Q1756fs",
"BRCA1 p.R1758K",
"c.5277+1G>A",
"BRCA1 p.M1775R",
"c.5342C>A p.A1708E",
"c.5348T>C p.Met1783Thr",
"BRCA1 c.5380G>C p.E1794Q",
"c.5385insC",
"BRCA1 c.5407-2A>T",
"p.V1810A",
"p.D1813E",
"p.R1835*",
"BRCA1 p.R1835P",
"c.5506G>A p.E1836K",
"c.5510G>C p.Trp1837Ser",
"BRCA1 p.E1849*",
"BRCA1 p.Y1853D",
"c.5622C>T p.R1835X",
"c.5622C>T p.R1835X",
"c.6326T>C p.V1736A",
"exons 21-24",
"Exon 1-13 del",
"deletion (exons 4-13)",
"EX6_7del",
"EX12_15del",
"exon 13 ins 6kb",
"EX20del",
"EX22_3’UTRdel",
"5’UTR_3’UTRdel",
"5’UTR_EX1del",
"5’UTR_EX2dup",
 ]
    list_variants = [s.replace('p.', '') for s in list_variants]
    list_variants = [s.replace('c.', '') for s in list_variants]
    list_variants = [s.replace('BRCA1', '') for s in list_variants]
    list_variants = [s.replace('BRCA', '') for s in list_variants]
    list_variants = [s.replace('BRCA2', '') for s in list_variants]
    list_variants = [s.replace('_', ' ') for s in list_variants]
    list_variants = [s.replace(',', ' ') for s in list_variants]
    terms_length = len(list_variants)-1
    term_number = 0
    not_found = []
    data_found = []
    data_not_found = []
    data_found_2 = []
    current_term_popped = ""
    start_term = list_variants[0]
    start_urls = [f"https://brcaexchange.org/backend/data/?format=json&order_by=Gene_Symbol&direction=ascending&page_size=1&page_num=0&search_term={start_term}&include=Variant_in_ENIGMA&include=Variant_in_ClinVar&include=Variant_in_1000_Genomes&include=Variant_in_ExAC&include=Variant_in_LOVD&include=Variant_in_BIC&include=Variant_in_ESP&include=Variant_in_exLOVD&include=Variant_in_ENIGMA_BRCA12_Functional_Assays&include=Variant_in_GnomAD&include=Variant_in_GnomADv3"]

    def parse(self, response):
        response_body = response.body
        response_json = json.loads(response_body.decode('utf-8'))

        is_done = isDone(self.term_number, self.terms_length)
        is_valid = validResult(response_json["count"])
        
        if is_valid:
            max_result = 0
            for variant in response_json["data"]:
            #variant = response_json["data"][0]
                if max_result <= 10:
                    variant_data = {
                        "term_searched": self.list_variants[self.term_number],
                        "protein_change": variant["Protein_Change"],
                        "cDNA_nomenclature": variant["HGVS_cDNA_LOVD"],
                        "BIC_Nomenclature": variant["BIC_Nomenclature"],
                        "HGVS_protein": variant["HGVS_Protein"],
                        "Protein_change": variant["Protein_Change"],
                        "Genomic_coordinate_hg38": variant["Genomic_Coordinate_hg38"],
                        "Genomic_Coordinate_hg37": variant["Genomic_Coordinate_hg37"],
                        "synonums": variant["Synonyms"]
                    }
                    self.data_found.append(variant_data)
                    max_result += 1

            if not is_done:
                self.term_number += 1
                next_page = BuildNextPage(self.list_variants[self.term_number])
                yield scrapy.Request(next_page, callback=self.parse,dont_filter=True)

        elif is_done:
            df_found = pd.DataFrame.from_dict(self.data_found)
            df_found_2 = pd.DataFrame.from_dict(self.data_found_2)
            df_not_found = pd.DataFrame.from_dict(self.data_not_found)
            df_found.to_excel('Data_Found.xlsx')
            df_found_2.to_excel('Data_Found_Second_search.xlsx')
            df_not_found.to_excel('Data_Not_Found.xlsx')
        
        else:
            self.not_found = self.list_variants[self.term_number].split()
            if (len(self.not_found) > 0):
                next_page = BuildNextPage(self.not_found[0])
                self.current_term_popped = self.not_found.pop(0)
                yield scrapy.Request(next_page, callback=self.DepthSearch,dont_filter=True)
            else:
                self.term_number += 1
                next_page = BuildNextPage(self.list_variants[self.term_number])
                yield scrapy.Request(next_page, callback=self.parse,dont_filter=True)
                
            
    def DepthSearch(self, response):
        response_body = response.body
        response_json = json.loads(response_body.decode('utf-8'))

        is_valid = validResult(response_json["count"])

        max_result = 0

        if is_valid:
            for variant in response_json["data"]:
            #variant = response_json["data"][0]
                if max_result <= 10:
                    variant_data = {
                        "original input": self.list_variants[self.term_number],
                        "term_searched": self.current_term_popped,
                        "protein_change": variant["Protein_Change"],
                        "cDNA_nomenclature": variant["HGVS_cDNA_LOVD"],
                        "BIC_Nomenclature": variant["BIC_Nomenclature"],
                        "HGVS_protein": variant["HGVS_Protein"],
                        "Protein_change": variant["Protein_Change"],
                        "Genomic_coordinate_hg38": variant["Genomic_Coordinate_hg38"],
                        "Genomic_Coordinate_hg37": variant["Genomic_Coordinate_hg37"],
                        "synonums": variant["Synonyms"]
                    }
                    self.data_found_2.append(variant_data)
                    max_result += 1
            
            if len(self.not_found) > 0:
                next_page = BuildNextPage(self.not_found[0])
                self.current_term_popped = self.not_found.pop(0)
                yield scrapy.Request(next_page, callback=self.DepthSearch,dont_filter=True)
            
            else:
                self.not_found = []  
                self.term_number += 1
                next_page = BuildNextPage(self.list_variants[self.term_number]) 
                yield scrapy.Request(next_page, callback=self.parse,dont_filter=True)
        
        elif len(self.not_found) > 0:
            next_page = BuildNextPage(self.not_found[0])
            self.current_term_popped = self.not_found.pop(0)
            yield scrapy.Request(next_page, callback=self.DepthSearch,dont_filter=True)  
        
        else:
            input_data = {
                "original_input": self.list_variants[self.term_number],
                "term_searched": self.current_term_popped,
            }
            self.data_not_found.append(input_data)
            self.term_number += 1
            self.not_found = []
            next_page = BuildNextPage(self.list_variants[self.term_number]) 
            yield scrapy.Request(next_page, callback=self.parse,dont_filter=True)