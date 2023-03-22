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

def BuildNextPage (term):
    term = urllib.parse.quote(term)
    return f"https://brcaexchange.org/backend/data/?format=json&order_by=Gene_Symbol&direction=ascending&page_size=1&page_num=0&search_term={term}&include=Variant_in_ENIGMA&include=Variant_in_ClinVar&include=Variant_in_1000_Genomes&include=Variant_in_ExAC&include=Variant_in_LOVD&include=Variant_in_BIC&include=Variant_in_ESP&include=Variant_in_exLOVD&include=Variant_in_ENIGMA_BRCA12_Functional_Assays&include=Variant_in_GnomAD&include=Variant_in_GnomADv3"

class CheckSpider(scrapy.Spider):
    name = "check_variants"
    list_variants = ["c.5266dupC",
    "c.5385insC",
    "c.1675delA",
    "c.1687C>T p.Q563*",
    "c.181T>G p.Cys61Gly",
    "c.300T>G p.C61G",
    "c.4327C>T p.Arg1443*",
    "c.5266dupC p.Q1756Pfs*74",
    "c.5277+1G>A",
    "c.68_69del p.Glu23Valfs*17",
    "c.68_69delAG",
    "c.IVS16+6T>C",
    "187delAG",
    "4184DEL4",
    "442-?_593+?del",
    "5’UTR_3’UTRdel",
    "5’UTR_EX1del",
    "5’UTR_EX2dup",
    "BRCA1 c.1030G>A p.Ala344Thr",
    "BRCA1 c.1674del p.Gly559Valfs*13",
    "BRCA1 c.181T>G p.Cys61Gly",
    "BRCA1 c.213-11T>G Intronic",
    "BRCA1 c.2999A>G p.Glu1000Gly",
    "BRCA1 c.3193dup p.Asp1065Glyfs*2",
    "BRCA1 c.3835G>A p.Ala1279Thr",
    "BRCA1 c.427G>T p.Glu143*",
    "BRCA1 c.4780C>G p.Pro1594Ala",
    "BRCA1 c.5036T>C p.Leu1679Pro",
    "BRCA1 c.815_824dup p.Thr276Alafs*14",
    "BRCA1 c.946A>G p.Ser316Gly",
    "BRCA1 c.962G>A p.Trp321*",
    "EX12_15del",
    "EX20del",
    "EX22_3’UTRdel",
    "EX6_7del",
    "Exon 1-13 del",
    "IVS 18-2delA",
    "IVS23-18T>A",
    "NM_007294.2",
    "c.1016dupA p.Val340GlyfsX6",
    "c.1082_1092del p.Ser361",
    "c.115T>C p.Cys39Arg",
    "c.128T>C",
    "c.1333G>C p.E445Q",
    "c.135-1G>T",
    "c.1360_1361del p.Ser454*",
    "c.1400A>G p.Lys467Arg",
    "c.1511G>A p.Arg504His",
    "c.1519A>G p.K467R",
    "c.1568T>G p.Leu523Trp",
    "c.1581G>C p.Lys527Asn",
    "c.1687C>T p.Gln563",
    "c.1687C>T p.Gln563Ter",
    "c.1806C>T p.Q563X",
    "c.181T>G c.181T>G (p.Cys61Gly)",
    "c.1820A>G p.Lys607Arg",
    "c.1875del p.Val626*",
    "c.187delAG",
    "c.1881C>G",
    "c.1881C>G",
    "c.1899A>G p.M594V",
    "c.18IT>G p.Cys61Gly",
    "c.1941T>G p.Ser647Arg",
    "c.1961delA",
    "c.1996C>A p.L666l",
    "c.202A>G (p.lle68VAL)",
    "c.2071delA p.Arg691AspfsTer10",
    "c.212+1G>A",
    "c.213-12A>G",
    "c.213-12A>G",
    "c.2154A>T p.K6789X",
    "c.2199del p.Lys734Asnfs*2",
    "c.230C>T p.Thr77Met",
    "c.2359dupG c.2359dupG (p.Glu787Glyfs 3)",
    "c.2411_2412del c.2411_2412del (p.Gln804Leufs*5)",
    "c.2411_2412delAG",
    "c.2457del p.Asp821llefs*25",
    "c.2475delC",
    "c.2518A>T p.S840C",
    "c.26delC (p.Pro9GlnfsX16)",
    "c.2771A>T p.N924l",
    "c.2836G>C",
    "c.2845G>A p.Gly949Ser",
    "c.2884G>A p.E962K",
    "c.2892A>C p.I925L",
    "c.300T>G p.C81G",
    "c.3181A>G. p.Ile1061Val",
    "c.3247A>C p.Met1083Leu",
    "c.3275A>G p.Glu1092Gly",
    "c.3289A>T p.S1097C",
    "c.3305A>G p.Asn1102Ser",
    "c.3367G>T p.Asp1123Tyr",
    "c.347A>G p.E116G",
    "c.3481_3491del11 p.Glu1161PhefsX3",
    "c.3573G>A p.D1152N",
    "c.3587C>T p.Thr1196lle",
    "c.3661G>T p.Glu1221*",
    "c.3700_3704del p.Val1234Glnfg=g",
    "c.3700_3704del p.Val1234Glnfs*g",
    "c.3700_3704delGTAAA",
    "c.3748G>T p.Glu1250*",
    "c.3756_3759del p.Ser1253Argfs*10",
    "c.3764dupA p.Asn1255Lysfs*12",
    "c.3783G>T p.E1222X",
    "c.3857G>T p.Ser1286lle",
    "c.4082T>C p.Met1361Thr",
    "c.4096+1G>A",
    "c.4129A>G p.S1377G",
    "c.4165_4166del p.Ser1389",
    "c.4327C>T p.Arg1443Ter",
    "c.4338A>T p.Glu1446Asp",
    "c.4339C>A p.Q1447k",
    "c.4357+1G>A",
    "c.4520G>C p.Arg1507Thr",
    "c.4643C>T c.4643C>T(p.Thr1548Met)",
    "c.4649C>T p.Thr1550lle",
    "c.4674A>G",
    "c.4689C>G p.Tyr1563Ter",
    "c.4868C>G p.Ala1623Gly",
    "c.4936delG",
    "c.4964_4982del p.Ser1655Tyrfs*16",
    "c.4964_4982del19",
    "c.4964_4982del19-report",
    "c.4986+6T>C",
    "c.499G>A p.Gly167Arg",
    "c.5023A>T c.5023A>T (p.T1675S",
    "c.5075-1G>A",
    "c.5096G>A p.Arg1699Gin",
    "c.5110T>C p.Phe1704Leu",
    "c.5123C>T p.A1708G",
    "c.515A>G p.Gln172Arg",
    "c.5177_5180del p.Arg1726Lysfs*3",
    "c.5177_5180delGAAA p.R1726Kfs*3",
    "c.5179A>T",
    "c.5179A>T p.Lys1727",
    "c.5179A>T p.Lys1727*",
    "c.5215G>A p.R1699Q",
    "c.525insA",
    "c.5266dup p.Gln1756Profs*74",
    "c.5266dupC",
    "c.5266dupC p.Gin1756Profs*74",
    "c.5266dupC p.Gln1756Profs*74",
    "c.5266dupC p.Gln1758Profs*74",
    "c.5342C>A p.A1708E",
    "c.5348T>C p.Met1783Thr",
    "c.5506G>A p.E1836K",
    "c.5510G>C p.Trp1837Ser",
    "c.5622C>T p.R1835X",
    "c.5622C>T p.R1835X",
    "c.6326T>C p.V1736A",
    "c.66dup p.Glu23Argfs*18",
    "c.697_698delGT p.Val233AsnfsX4",
    "c.7592_7595delITTCCins10 p.Val2531_Pro2532delinsAVGG",
    "c.798_799delTT p.Ser267Lysfs*19",
    "c.811G>C p.V271L",
    "c.813G>A p.D232N",
    "c.815_824dup10",
    "c.815_824dupAGCCATGTGG p.Thr276Alafs*14",
    "c.8850G>T p.Lys2950Asn",
    "c.IVF5-1 G>A",
    "c.IVS1-17_IVS1-13delTTTCTInsAA",
    "c.IVS1-17_IVS1-13delTTTCTinsAA",
    "c.IVS10-11T>G",
    "c.IVS8+8T>G",
    "c.c1061A>G",
    "c.c1418A>T p.Asn47lle",
    "c.c1703C>T p.P568L",
    "c.c2246A>T p.Asp749Val",
    "deletion (exons 4-13)",
    "exon 13 ins 6kb",
    "exons 21-24",
    "p.1835",
    "p.A102G",
    "p.Asn1029Argfs*5",
    "p.Asn354Ser",
    "p.C39R",
    "p.C61G",
    "p.Cys61Gly",
    "p.D1813E",
    "p.E1134",
    "p.Glu23Valfs*17",
    "p.K1727*",
    "p.K719E",
    "p.L512F",
    "p.L523V",
    "p.L999R",
    "p.M1173V",
    "p.N3124l",
    "p.Q1273",
    "p.Q1395*",
    "p.Q1538",
    "p.Q563*",
    "p.R1835*",
    "p.S1217P",
    "p.T1550I",
    "p.T1675S",
    "p.T77M",
    "p.Tyr422His",
    "p.V1181l",
    "p.V1810A",
    "p.V47F",
    "BRCA1",
    "BRCA1",
    "BRCA1 c.2657C>G p.S886C"
    ]
    list_variants = [s.replace('p.', '') for s in list_variants]
    list_variants = [s.replace('c.', '') for s in list_variants]
    terms_length = len(list_variants)-1
    term_number = 0
    not_found = []
    data_found = []
    data_not_found = []
    data_found_2 = []
    start_term = BuildNextPage(list_variants[term_number])
    start_urls = [f"https://brcaexchange.org/backend/data/?format=json&order_by=Gene_Symbol&direction=ascending&page_size=1&page_num=0&search_term={start_term}&include=Variant_in_ENIGMA&include=Variant_in_ClinVar&include=Variant_in_1000_Genomes&include=Variant_in_ExAC&include=Variant_in_LOVD&include=Variant_in_BIC&include=Variant_in_ESP&include=Variant_in_exLOVD&include=Variant_in_ENIGMA_BRCA12_Functional_Assays&include=Variant_in_GnomAD&include=Variant_in_GnomADv3"]

    def parse(self, response):
        response_body = response.body
        response_json = json.loads(response_body.decode('utf-8'))


        is_done = isDone(self.term_number, self.terms_length)
        is_valid = validResult(response_json["count"])
        
        print(is_done)
        if is_valid:
            #for variant in response_json["data"]:
            variant = response_json["data"][0]
            
            variant_data = {
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
            self.term_number += 1
            if not is_done:
                next_page = BuildNextPage(self.list_variants[self.term_number])
                yield scrapy.Request(next_page, callback=self.parse,dont_filter=True)

        if is_done:
            print("<<<<<>>>>")
            print (self.data_not_found)
            print(self.data_found_2)
            print (self.data_found)
        
        else:
            self.not_found = self.list_variants[self.term_number].split()
            self.not_found = [i for i in self.not_found if not i.startswith('BRCA')]
            next_page = BuildNextPage(self.not_found[0])
            self.not_found.pop(0)
            yield scrapy.Request(next_page, callback=self.DepthSearch,dont_filter=True)  
            
    
            # df_found = pd.DataFrame.from_dict(self.data_found)
            # df_found_2 = pd.DataFrame.from_dict(self.data_found_2)
            # df_not_found = pd.DataFrame.from_dict(self.data_not_found)
            # df_found.to_excel('Data_Found.xlsx')
            # df_found_2.to_excel('Data_Found_Second_search.xlsx')
            # df_not_found.to_excel('Data_Not_Found.xlsx')

    def DepthSearch(self, response):
        response_body = response.body
        response_json = json.loads(response_body.decode('utf-8'))

        is_valid = validResult(response_json["count"])

        if is_valid:
            #for variant in response_json["data"]:
            variant = response_json["data"][0]
            variant_data = {
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
            self.not_found = []  
            self.term_number += 1
            next_page = BuildNextPage(self.list_variants[self.term_number]) 
            yield scrapy.Request(next_page, callback=self.parse,dont_filter=True)
        
        elif len(self.not_found) > 0:
            print(len(self.not_found) > 0)
            print(self.not_found[0])
            print("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            next_page = BuildNextPage(self.not_found[0])
            self.not_found.pop(0)
            yield scrapy.Request(next_page, callback=self.DepthSearch,dont_filter=True)  
        
        else:
            self.data_not_found.append(self.list_variants[self.term_number])
            self.term_number += 1
            next_page = BuildNextPage(self.list_variants[self.term_number]) 
            yield scrapy.Request(next_page, callback=self.parse,dont_filter=True)