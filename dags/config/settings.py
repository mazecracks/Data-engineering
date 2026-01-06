# config
key = "all"
sub_key = "all"
base_url = "https://api.imf.org/external/sdmx/2.1/data"
flows = {
    "BOP":  "IMF.STA,BOP,latest",
    "IMTS": "IMF.STA,IMTS,latest",
    "CPI":  "IMF.STA,CPI,latest",
}
startPeriod = 1948
endPeriod = 2025

output_dir = "imf_downloads"

BUCKET = 'personal-projectbucket'
S3_KEY = 'personal-projectbucket/data'
AWS_S3_PREFIX='imf'