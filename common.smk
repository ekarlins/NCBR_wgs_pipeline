#!/usr/bin/env python

def get_prev_gvcf(wildcards):
    if int(last_batch) == 0:
        return "../cumulative/batch0.g.vcf.gz"
    return "../cumulative/through_batch" + last_batch + "_" + wildcards.chunks + ".g.vcf.gz"
