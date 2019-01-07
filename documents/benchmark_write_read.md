# 思路

控制变量 | 范围
--- | ---
order | 顺序写，随机写(random shuffle)
buffer-size | `{1,2,4,8,16}` `*` `4KB`

* 顺序 vs 随机写 value no-fallocate

```
11:16:30 156:[0m Close the database.
11:16:30 412:[0m Close file..
11:16:30 426:[0m 0
11:18:26 352:[0m Step one File exists, 508,500, 116.089
11:18:26 426:[0m 1
11:20:21 352:[0m Step one File exists, 506,536, 114.852
11:20:21 426:[0m 2
11:22:15 352:[0m Step one File exists, 508,584, 114.419
11:22:15 426:[0m 3
11:24:10 352:[0m Step one File exists, 510,688, 114.930
11:24:10 426:[0m 4
11:26:06 352:[0m Step one File exists, 510,824, 115.395
11:26:06 436:[0m Close file end..
11:26:06 457:[0m 0
11:28:02 352:[0m Step one File exists, 513,432, 116.023
11:28:02 457:[0m 1
11:29:57 352:[0m Step one File exists, 513,432, 114.868
11:29:57 457:[0m 2
11:31:51 352:[0m Step one File exists, 513,432, 114.491
11:31:51 457:[0m 3
11:33:46 352:[0m Step one File exists, 513,432, 114.862
11:33:46 457:[0m 4
11:35:41 352:[0m Step one File exists, 513,432, 115.382
11:35:41 185:[0m Close the database successfully.
```

*  顺序写 no-fallocate

```
17:34:27 180:[0m Close the database.
17:34:27 480:[0m Close file..
17:34:27 494:[0m 0
17:34:27 355:[0m rm -r test_directory/*
17:38:26 414:[0m Step one File exists, 508,592, 238.938
17:38:26 426:[0m Step one File exists, 508,592, 238.992 end.
17:40:16 458:[0m Step two File exists, 508,592, 109.900
17:40:16 494:[0m 1
17:40:16 355:[0m rm -r test_directory/*
17:42:12 414:[0m Step one File exists, 506,752, 115.441
17:42:12 426:[0m Step one File exists, 506,752, 115.443 end.
17:44:02 458:[0m Step two File exists, 506,936, 109.898
17:44:02 494:[0m 2
17:44:02 355:[0m rm -r test_directory/*
17:45:56 414:[0m Step one File exists, 506,936, 114.264
17:45:56 426:[0m Step one File exists, 506,936, 114.273 end.
17:47:42 458:[0m Step two File exists, 506,936, 105.959
17:47:42 494:[0m 3
17:47:42 355:[0m rm -r test_directory/*
17:49:37 414:[0m Step one File exists, 506,980, 114.714
17:49:37 426:[0m Step one File exists, 506,980, 114.724 end.
17:51:18 458:[0m Step two File exists, 507,132, 100.984
17:51:18 494:[0m 4
17:51:18 355:[0m rm -r test_directory/*
17:53:14 414:[0m Step one File exists, 507,344, 115.299
17:53:14 426:[0m Step one File exists, 507,344, 115.305 end.
17:54:52 458:[0m Step two File exists, 508,928, 98.516
17:54:52 215:[0m Close the database successfully.
```

* 顺序写 fallocate (2018-11-07_20-15-write-kv-fallocate)

```
20:02:05 180:[0m Close the database.
20:02:05 483:[0m Close file..
20:02:05 497:[0m 0
20:02:05 355:[0m rm -r test_directory/*
20:02:05 377:[0m Release File exists, 508,420, 0.263
20:04:15 417:[0m Step one File exists, 508,576, 130.198
20:04:16 429:[0m Step one File exists, 508,576, 130.241 end.
20:04:16 497:[0m 1
20:04:16 355:[0m rm -r test_directory/*
20:04:16 377:[0m Release File exists, 506,844, 0.183
20:06:10 417:[0m Step one File exists, 507,132, 114.513
20:06:10 429:[0m Step one File exists, 507,132, 114.551 end.
20:06:10 497:[0m 2
20:06:10 355:[0m rm -r test_directory/*
20:06:11 377:[0m Release File exists, 507,148, 0.247
20:08:05 417:[0m Step one File exists, 507,260, 113.964
20:08:05 429:[0m Step one File exists, 507,260, 113.971 end.
20:08:05 497:[0m 3
20:08:05 355:[0m rm -r test_directory/*
20:08:05 377:[0m Release File exists, 507,420, 0.209
20:10:00 417:[0m Step one File exists, 507,668, 114.389
20:10:00 429:[0m Step one File exists, 507,668, 114.393 end.
20:10:00 497:[0m 4
20:10:00 355:[0m rm -r test_directory/*
20:10:00 377:[0m Release File exists, 507,864, 0.268
20:11:55 417:[0m Step one File exists, 508,156, 114.736
20:11:55 429:[0m Step one File exists, 508,156, 114.744 end.
20:11:55 215:[0m Close the database successfully.
```

* 随机和顺序读写

```
00:53:54 :159:[0m Close the database.
00:53:54 :431:[0m Close file..
00:53:54 :445:[0m 0
00:55:49 :368:[0m Step one File exists, 508,516, 115.656
00:55:49 :377:[0m Step one File exists, 508,516, 115.657 end.
00:57:39 :409:[0m Step two File exists, 508,580, 109.911
00:57:39 :445:[0m 1
00:59:34 :368:[0m Step one File exists, 514,656, 114.454
00:59:34 :377:[0m Step one File exists, 514,656, 114.455 end.
01:01:23 :409:[0m Step two File exists, 515,016, 109.804
01:01:23 :445:[0m 2
01:03:18 :368:[0m Step one File exists, 515,016, 114.241
01:03:18 :377:[0m Step one File exists, 515,016, 114.242 end.
01:05:04 :409:[0m Step two File exists, 515,016, 106.102
01:05:04 :445:[0m 3
01:06:58 :368:[0m Step one File exists, 515,036, 114.576
01:06:58 :377:[0m Step one File exists, 515,036, 114.577 end.
01:08:39 :409:[0m Step two File exists, 515,192, 101.067
01:08:39 :445:[0m 4
01:10:35 :368:[0m Step one File exists, 515,308, 115.066
01:10:35 :377:[0m Step one File exists, 515,308, 115.066 end.
01:12:13 :409:[0m Step two File exists, 517,124, 98.652
01:12:13 :455:[0m Close file end..
01:12:13 :476:[0m 0
01:14:09 :368:[0m Step one File exists, 518,108, 115.630
01:14:09 :377:[0m Step one File exists, 518,108, 115.630 end.
01:15:59 :409:[0m Step two File exists, 518,108, 109.734
01:15:59 :476:[0m 1
01:17:53 :368:[0m Step one File exists, 518,108, 114.688
01:17:53 :377:[0m Step one File exists, 518,108, 114.688 end.
01:19:43 :409:[0m Step two File exists, 518,108, 110.020
01:19:43 :476:[0m 2
01:21:38 :368:[0m Step one File exists, 518,108, 114.349
01:21:38 :377:[0m Step one File exists, 518,108, 114.349 end.
01:23:24 :409:[0m Step two File exists, 518,108, 105.950
01:23:24 :476:[0m 3
01:25:18 :368:[0m Step one File exists, 518,108, 114.743
01:25:18 :377:[0m Step one File exists, 518,108, 114.743 end.
01:27:00 :409:[0m Step two File exists, 518,108, 101.057
01:27:00 :476:[0m 4
01:28:55 :368:[0m Step one File exists, 518,108, 115.197
01:28:55 :377:[0m Step one File exists, 518,108, 115.197 end.
01:30:33 :409:[0m Step two File exists, 518,108, 98.599
01:30:33 :194:[0m Close the database successfully.
```

* 随机和顺序总结
    * buffer-size足够大随机和顺序的读写性能相近
    * 顺序和随机对read影响更大, 对write影响相对较小

* write-benchmark总结
    * 16KB buffer写入最好
    * fallocate与否总时间差别不大 (加上`0.263s` fallocate time), `4KB` no fallocate very bad, `8KB` no fallocte ok

* read-benchmark总结
    * read `block size / buffer size` 一次较大更好