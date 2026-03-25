# 🔍 Check Operators Analysis Report

## 📊 Summary

- Total rules without check operators: 302
- Total unique operators found in rules: 58
- Total implemented PostgreSQL operators: 86
- Missing operators (in rules but not implemented): 1
- Extra operators (implemented but not used in rules): 29

## ✅ Implementation Status

### ❌ Missing Operators (Need Implementation)

| Operator                 | Status             |
| ------------------------ | ------------------ |
| `is_not_valid_reference` | ❌ Not Implemented |

## 🔍 Unused Implemented Operators

The following operators are implemented but not currently used in any rules:

```
- conformant_value_data_type
- contains_all
- contains_case_insensitive
- does_not_contain_case_insensitive
- does_not_reference_correct_codelist
- equals_string_part
- has_different_values
- has_equal_length
- has_next_corresponding_record
- is_not_ordered_by
- is_not_ordered_set
- is_not_valid_whodrug_reference
- is_ordered_set
- is_unique_relationship
- is_valid_whodrug_reference
- longer_than_or_equal_to
- non_conformant_value_data_type
- non_conformant_value_length
- non_empty_within_except_last_row
- prefix_is_contained_by
- shares_at_least_one_element_with
- shares_exactly_one_element_with
- shorter_than_or_equal_to
- suffix_equal_to
- suffix_is_contained_by
- suffix_not_equal_to
- target_is_sorted_by
- value_does_not_have_multiple_references
- variable_metadata_not_equal_to
```

## 📋 Rules Without Check Operators

| Core ID              | CDISC Rule ID                       | Standard                                                                         | Status    | In Cache |
| -------------------- | ----------------------------------- | -------------------------------------------------------------------------------- | --------- | -------- |
| CDISC.ADAMIG.111     | 111                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.252     | 252                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0019  | 19                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0020  | 20                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0021  | 21                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0022  | 22                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0023  | 23                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0024  | 24                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0025  | 25                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0026  | 26                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0027  | 27                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0028  | 28                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0029  | 29                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0030  | 30                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0031  | 31                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0032  | 32                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0039  | AD0039                              | ADAMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0041  | 41                                  | ADAMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0046  | 46                                  | ADAMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0047  | 47                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0053  | 53                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0061  | AD0061                              | ADAMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0088  | 88                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0089  | 89                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0261  | 261                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0262  | 262                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0278  | 278                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0620  | 620                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0621  | 621                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0623  | 623                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0624  | 624                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0625  | 625                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0626  | 626                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0627  | 627                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0628  | 628                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0629  | 629                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0630  | 630                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0631  | 631                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0632  | 632                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADAMIG.AD0633  | 633                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0005  | 5                                   | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0006  | 6                                   | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0007  | 7                                   | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0013  | 13                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0014  | 14                                  | Model v2.1, ADaMIG                                                               | Draft     | ❌       |
| CDISC.ADaMIG.AD0016  | 16                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0017  | 17                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0049  | 49                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0054  | 54                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0060  | 60                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0143  | 143                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0144  | 144                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0145  | 145                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0159  | 159                                 | ADaMIG, ADaMIG-MD                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0160  | 160                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0163  | 163                                 | ADaMIG, ADaMIG-MD                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0164  | 164                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0166  | 166                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0167  | 167                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0169  | 169                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0176  | 176                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0178  | 178                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0194  | 194                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0195  | 195                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0196  | 196                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0197  | 197                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0198  | 198                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0204  | 204                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0211  | 211                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0252b | 252                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0254  | 254                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0254b | 254                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0268  | 268                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0269  | 269                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0270  | 270                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0271  | 271                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0279  | 279                                 | ADaMIG, ADaMIG-MD, OCCDS                                                         | Draft     | ❌       |
| CDISC.ADaMIG.AD0363  | 363                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0364  | 364                                 | ADaMIG-MD, OCCDS                                                                 | Draft     | ❌       |
| CDISC.ADaMIG.AD0373  | 373                                 | ADTTE                                                                            | Draft     | ❌       |
| CDISC.ADaMIG.AD050   | 50                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD051   | 51                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD052   | 52                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0522b | 522                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0523b | 523                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0524  | 524                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0525  | 525                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0529  | 529                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD0530  | 530                                 | ADaMIG-MD, ADaMIG                                                                | Draft     | ❌       |
| CDISC.ADaMIG.AD055   | 55                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0619  | 619                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD064   | 64                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD071   | 71                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD072   | 72                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD080   | 80                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0800  | 800                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0802  | 802                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0804  | 804                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0805  | 805                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0806  | 806                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0807  | 807                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0809  | 809                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0811  | 811                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0812  | 812                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0813  | 813                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0814  | 814                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0815  | 815                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0816  | 816                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.ADaMIG.AD0817  | 817                                 | ADaMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0001  | CG0001, TIG0289, SEND16, TIG0090    | SDTMIG, TIG, SENDIG, SENDIG-DART, SENDIG-GENETOX                                 | Draft     | ❌       |
| CDISC.SDTMIG.CG0002  | CG0002                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0010  | CG0010                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0012  | CG0012, SEND4                       | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART                                      | Draft     | ❌       |
| CDISC.SDTMIG.CG0015  | CG0015                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0019  | CG0019                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0096  | CG0096                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0097  | CG0097                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0103  | CG0103                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0104  | CG0104                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0155  | CG0155                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0156  | CG0156                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0157  | CG0157                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0158  | CG0158                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0159  | CG0159                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0160  | CG0160                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0161  | CG0161                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0162  | CG0162                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0167  | CG0167                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0169  | CG0169                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0173  | CG0173                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0184  | CG0184                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0186  | CG0186                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0187  | CG0187                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0188  | CG0188                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0189  | CG0189                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0190  | CG0190                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0192  | CG0192                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0196  | CG0196                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0197  | CG0197                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0198  | CG0198                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0236  | CG0236                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0237  | CG0237                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0238  | CG0238                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0249  | CG0249                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0250  | CG0250                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0251  | CG0251                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0252  | CG0252                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0253  | CG0253                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0254  | CG0254                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0267  | CG0267                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0288  | CG0288                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0295  | CG0295                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0296  | CG0296                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0298  | CG0298                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0312  | CG0312                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0320  | CG0320                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0321  | CG0321                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0322  | CG0322                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0323  | CG0323                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0324  | CG0324                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0347  | CG0347                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0351  | CG0351                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0375  | CG0375                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0377  | CG0377                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0378  | CG0378                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0379  | CG0379                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0380  | CG0380                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0381  | CG0381                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0382  | CG0382                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0383  | CG0383                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0384  | CG0384                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0385  | CG0385                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0386  | CG0386                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0400  | CG0400                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0418  | CG0418                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0425  | CG0425                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0431  | CG0431                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0436  | CG0436                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0437  | CG0437                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0437  | CG0442                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0443  | CG0443                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0445  | CG0445                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0446  | CG0446                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0448  | CG0448                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0449  | CG0449                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0450  | CG0450                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0451  | CG0451                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0453  | CG0453                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0454  | CG0454                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0460  | CG0460                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0461  | CG0461                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0463  | CG0463                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0464  | CG0464                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0466  | CG0466                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0523  | CG0523                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0524  | CG0524                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0525  | CG0525                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0526  | CG0526                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0527  | CG0527                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0532  | CG0532                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0546  | CG0546                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0548  | CG0548                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0551  | CG0551                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0555  | CG0555                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0556  | CG0556                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0557  | CG0557                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0558  | CG0558                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0559  | CG0559                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0560  | CG0560                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0562  | CG0562, TIG0648                     | SDTMIG, TIG                                                                      | Draft     | ❌       |
| CDISC.SDTMIG.CG0563  | CG0563                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0604  | CG0604                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0605  | CG0605                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0607  | CG0607                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0608  | CG0608                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0609  | CG0609                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0610  | CG0610                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0614  | CG0614                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0615  | CG0615                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0616  | CG0616                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0617  | CG0617                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0651  | CG0651                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SDTMIG.CG0663  | CG0663                              | SDTMIG                                                                           | Draft     | ❌       |
| CDISC.SENDIG.296     | FB1701, SEND296                     | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| CDSIC.ADaMIG.AD0010  | 10                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDSIC.ADaMIG.AD0070  | 70                                  | ADaMIG                                                                           | Draft     | ❌       |
| CDSIC.ADaMIG.AD0112  | 112                                 | ADaMIG, ADaMIG-MD                                                                | Draft     | ❌       |
| CDSIC.ADaMIG.AD0113  | 113                                 | ADaMIG, ADaMIG-MD                                                                | Draft     | ❌       |
| CORE-000008          | CG0132, FB0601                      | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000205          | CG0399                              | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000238          | CG0147, TIG0388                     | SDTMIG, TIG                                                                      | Published | ❌       |
| CORE-000239          | CG0148, TIG0389                     | SDTMIG, TIG                                                                      | Published | ❌       |
| CORE-000343          | CG0011                              | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000344          | CG0020                              | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000345          | CG0021                              | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000346          | CG0047                              | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000347          | CG0071, TIG0340                     | SDTMIG, TIG                                                                      | Draft     | ❌       |
| CORE-000348          | CG0076                              | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000349          | CG0098                              | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000350          | CG0099                              | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000366          | CG0109                              | SDTMIG                                                                           | Published | ❌       |
| CORE-000560          | 1                                   | ADaMIG                                                                           | Published | ❌       |
| CORE-000581          | CG0368, TIG0532, TRC1736a, TRC1736c | SDTMIG, TIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                      | Published | ❌       |
| CORE-000739          | CG0407                              | SDTMIG                                                                           | Published | ❌       |
| CORE-000744          | CG0174, TIG0403                     | SDTMIG, TIG                                                                      | Published | ❌       |
| CORE-000748          | FB2504                              | SDTMIG                                                                           | Draft     | ❌       |
| CORE-000852          | CG0330, CG0664, TIG0698, SEND48     | SDTMIG, TIG, SENDIG, SENDIG-GENETOX, SENDIG-DART                                 | Published | ❌       |
| CORE-000853          | FB1602                              | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ❌       |
| CORE-000862          | FB3205                              | SDTMIG, SENDIG, SENDIG-AR, SENDIG-DART, SENDIG-GENETOX                           | Published | ❌       |
| CORE-000863          | FB3101                              | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Published | ❌       |
| CORE-000864          | FB3206                              | SDTMIG, SENDIG, SENDIG-AR, SENDIG-DART, SENDIG-GENETOX                           | Published | ❌       |
| CORE-000865          | FB4405                              | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Published | ❌       |
| CORE-000866          | FB3210                              | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Published | ❌       |
| CORE-000867          | FB1501                              | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Published | ❌       |
| FB2602               | FB2602                              | SDTMIG                                                                           | Draft     | ❌       |
| FDA.SDTMIG.CT2001    | FDAB017                             | SDTMIG                                                                           | Draft     | ❌       |
| FDA.SDTMIG.SD1097    | FB0101                              | SDTMIG                                                                           | Draft     | ❌       |
| FDA.TRC1734a         | TRC1734a                            | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| FDA.TRC1734b         | TRC1734                             | SDTMIG, SENDIG, SENDIG-AR, SENDIG-DART                                           | Draft     | ❌       |
| NicTest              | Demo                                | SDTMIG                                                                           | Draft     | ❌       |
| WHODrug              | WHODrug                             | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB1502                              | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Draft     | ❌       |
| unknown              | FB3102                              | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Draft     | ❌       |
| unknown              | FB2303                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB1107                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB2305                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB0611                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB2301                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB2302                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB4301                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB2304                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB2306                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB3211                              | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB6901                              | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB2503                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB0609                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB4006                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB3801                              | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB1801                              | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB1901                              | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB0405                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB1112                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB1109                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB1108                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB7101                              | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB3405                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB3407                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB3403                              | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Draft     | ❌       |
| unknown              | FB3402                              | SDTMIG, SENDIG, SENDIG-DART, SENDIG-AR, SENDIG-GENETOX                           | Draft     | ❌       |
| unknown              | FB0501                              | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FB0913                              | SDTMIG, SENDIG, SENDIG-GENETOX, SENDIG-DART, SENDIG-AR                           | Draft     | ❌       |
| unknown              | FB2604J                             | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | FULL_TEMPLATE,                      | ADaMIG, USDM, SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, SENDIG-AR, Define-XML | Draft     | ❌       |
| unknown              | NICRULE.MEDDRA                      | SDTMIG                                                                           | Draft     | ❌       |
| unknown              | 895                                 | ADaMIG                                                                           | Draft     | ❌       |
| unknown              | FULL_TEMPLATE,                      | ADaMIG, USDM, SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, SENDIG-AR, Define-XML | Draft     | ❌       |
| unknown              | FULL_TEMPLATE,                      | ADaMIG, USDM, SDTMIG, SENDIG, SENDIG-DART, SENDIG-GENETOX, SENDIG-AR, Define-XML | Draft     | ❌       |
