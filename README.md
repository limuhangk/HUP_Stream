# HUP_Stream
Note: This is the implementation of HUP_Stream algorithm.

This repository contains the source code for the paper "High utility pattern mining algorithm over data streams using ext‑list", published in Applied Intelligence journal. For more information, please contact the main author.

Citation: Han M, Li M, Chen Z, et al. High utility pattern mining algorithm over data streams using ext-list[J]. Applied Intelligence, 2023: 1-24.
https://doi.org/10.1007/s10489-023-04925-6.

Abstract: High utility pattern has received a lot of research and attention because of their wide range of application scenarios. How to efficiently mine high utility patterns over data streams has become an important issue in the field of data mining. To solve the problem that the traditional utility list structure has too many join operations and the join operation is not efficient, which leads to the low spatio-temporal efficiency of the algorithm and the problem that the sliding window model repeatedly generates the same resultset, a new algorithm for high utility pattern mining over data streams is proposed, named HUPM_Stream. A location-indexed list structure, Ext-list, is designed to reduce the time complexity of the utility list join operation, and an improved remaining utility pruning strategy IRS is proposed to reduce the number of utility list join operations, and a hash table structure-based resultset maintenance strategy HRS is designed to effectively reduce the search space of the algorithm and avoid repeatedly generating the same resultset during the sliding process of the window. A large number of experimental results show that the proposed algorithm has better performance on dense datasets.
