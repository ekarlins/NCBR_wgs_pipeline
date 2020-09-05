# NCBR_wgs_pipeline

To run:

1. Clone down repository into working directory where your pipeline will run and write output

2. From within the working directory, run the following:
    NCBR_wgs_pipeline/ncbr_wgs_rapid.sh /path/to/rawdata/ align|varcall|qc npr|process hg19|hg38
    
    align steps generate variant calling-ready BAM file, varcall steps call variants, and QC generates QC and report
    
    'npr' runs a dry run, 'process' begins submitting jobs
