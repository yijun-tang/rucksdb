# rucksdb
RocksDB Re-implemention in Rust for Learning Purpose. Just focusing on the core algorithms and basic priciples.

## The statistics of LOC for v1.5.7:

```txt
===============================================================================
 Language            Files        Lines         Code     Comments       Blanks
===============================================================================
 Autoconf                5         1994         1627          126          241
 Automake                2           51           23           20            8
 C                       3          854          706           94           54
 C Header              257        49896        26455        15148         8293
 C++                   101        40692        32778         2855         5059
 CSS                     1           89           78            1           10
 Forth                   1           12            8            0            4
 Java                   38         4653         2718         1237          698
 Lisp                    1           94           81            1           12
 Makefile                2         1179          969           58          152
 Markdown                4          332            0          245           87
 PHP                     5          343          283           12           48
 Shell                  10        19525        15041         2763         1721
 Plain Text             14        26645            0        23335         3310
 Thrift                  9          678          391          167          120
 Visual Studio Pro|      1          194          193            1            0
 XML                    11         1546         1120          303          123
-------------------------------------------------------------------------------
 HTML                    4         1794         1690            0          104
 |- CSS                  1           72           56            1           15
 (Total)                           1866         1746            1          119
===============================================================================
 Total                 469       150571        84161        46366        20044
===============================================================================
```
Note: the core directories `db`, `util`, `include`, and `table` contains 20000~ LOC in total.
