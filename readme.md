# COSTREAM


This is the source code of our (Roman Heinrich, Carsten Binnig, Harald Kornmayer and Manisha Luthra) published paper at ICDE 2024: "COSTREAM: Learned Cost Models for Operator Placement in Edge-Cloud Environments"

## Citation

Please cite our papers, if you find this work useful or use it in your paper as a baseline.

```
@inproceedings{heinrich2024costream,
  author={Heinrich, Roman and Binnig, Carsten and Kornmayer, Harald and Luthra, Manisha},
  title={COSTREAM: Learned Cost Models for Operator Placement in Edge-Cloud Environments},
  year = {2024},
  booktitle={40th IEEE International Conference on Data Engineering (ICDE)},
  pages = {1–14},
  numpages = {14},
  url = {https://arxiv.org/pdf/2403.08444}
}

@inproceedings{heinrich2022debs,
  author = {Heinrich, Roman and Luthra, Manisha and Kornmayer, Harald and Binnig, Carsten},
  title = {Zero-shot cost models for distributed stream processing},
  year = {2022},
  isbn = {9781450393089},
  url = {https://doi.org/10.1145/3524860.3539639},
  doi = {10.1145/3524860.3539639},
  booktitle = {Proceedings of the 16th ACM International Conference on Distributed and Event-Based Systems},
  pages = {85–90},
  numpages = {6},
  series = {DEBS '22}
}
```

## Abstract
In this work, we present COSTREAM, a novel learned cost model for Distributed Stream Processing Systems that provides accurate predictions of the execution costs of
a streaming query in an edge-cloud environment. The cost model can be used to find an initial placement of operators across heterogeneous hardware, which is particularly important in these environments. 
In our evaluation, we demonstrate that COSTREAM can produce highly accurate cost estimates for the initial operator placement and even generalize to unseen placements, queries, and hardware. 
When using COSTREAM to optimize the placements of streaming operators, a median speed-up of around 21× can be achieved compared to baselines.

## Overview
COSTREAM consists out of three separate packages. Each of these have a separate README.
1. [COSTREAM Management](/costream-management/README.md): Code for setting up distributed clusters and the collection of training data
1. [COSTREAM Plan Generation](/costream-plan-generation/readme.md): Code for generate and execute DSPS queries with Apache Storm v.2.4.0
1. [COSTREAM Learning](/costream-learning/README.md): Code for learning and inference of COSTREAM
---

