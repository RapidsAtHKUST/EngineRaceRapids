## Disclaimer

此项目仅供学习与交流使用，请遵循MIT协议，如果您在任何项目中使用相关代码，请保留此项目的LICENSE文件。

## 文档

说明 | 文件
--- | ---
Code-Review | [documents/code-review.md](documents/code-review.md)
比赛攻略 | [documents/contest_summary_toc.md](documents/contest_summary_toc.md)
PDF-比赛攻略 | [release-pdf-documents/Rapids-Contest-Summary.pdf](release-pdf-documents/Rapids-Contest-Summary.pdf)
PDF-答辩PPT | [release-pdf-documents/Rapids-Defense-PPT.pdf](release-pdf-documents/Rapids-Defense-PPT.pdf)

## 目录结构

项目代码在两个目录中 [engine_race](engine_race) 和 [playground](playground)。
浏览代码可以用[clion](https://www.jetbrains.com/clion/)打开项目看。

### KV-DB引擎实现的代码

文件 | 说明
--- | ---
[engine_race/barrier.h](engine_race/barrier.h) | 可以重复使用的barrier，通过generation来支持
[engine_race/blocking_queue.h](engine_race/blocking_queue.h) | 简单的blocking-queue实现 (没有任何性能考虑的)
[engine_race/concurrentqueue.h](engine_race/concurrentqueue.h), [engine_race/blockingconcurrentqueue.h](engine_race/blockingconcurrentqueue.h) | 工业级别的blocking-queue实现 [cameron314/concurrentqueue](https://github.com/cameron314/concurrentqueue)，具体使用参考两个文件中的license
[engine_race/file_util.h](engine_race/file_util.h) | 文件相关的封装
[engine_race/util.h](engine_race/util.h) | 统计性能调优的信息: `dstat`, `iostat`, 内存占用; 打印timestamp和对应代码行位置util
[engine_race/log.h](engine_race/log.h), [engine_race/log.cc](engine_race/log.cc) | log工具, 稍做修改
[engine_race/engine_race.h](engine_race/engine_race.h), [engine_race/engine_race.cc](engine_race/engine_race.cc) | KV-DB具体实现

* 曾经使用的文件

文件 | 说明
--- | ---
[engine_race/thread_pool.h](engine_race/thread_pool.h) | 简单的线程池实现 [progschj/ThreadPool](https://github.com/progschj/ThreadPool), 在之前版本中使用, 后来改用`std::thread`, `std::promise`, `std::future`代替了
[engine_race/sparsepp](engine_race/sparsepp) | 高效的sparese hashmap实现，特点: 空间占用少, [greg7mdp/sparsepp](https://github.com/greg7mdp/sparsepp)

* 官方头文件

请见[include](include), 主要有`engine`和`polar_string`的定义。

### 测试的代码

文件 | 说明
--- | ---
[playground/test_engine.cpp](playground/test_engine.cpp) | 测试代码, 三阶段分别使用不同的omp线程池

### 官方的样例代码

目录 | 说明
--- | ---
[engine_example](engine_example) | 官方样例主要代码
[test](test) | 官方样例测试代码


### Cmake Config文件

文件 | 说明
--- | ---
[CMakeLists.txt](CMakeLists.txt) | 根cmakelist
[playground/CMakeLists.txt](playground/CMakeLists.txt) | playground cmakelist

## 本地测试使用

```zsh
mkdir build && cd build 
cmake ..
make -j
./playground/test_engine_nonoff        
```

* 注意: [playground/test_engine.cpp](playground/test_engine.cpp)中的DB路径`/DataRapids/`需要存在, 请将SSD设备挂载到这个路径。

### 官方的Makefile build

请见[Makefile](Makefile)和[engine_race/Makefile](engine_race/Makefile), 生成的静态链接库在`./lib`目录下。

```zsh
make -C . TARGET_ENGINE=engine_race
```

## 最终线上效果

* 历史最佳成绩: `413.69 seconds`

* 进程elapsed time

```
写入进程的历史最佳状态: 114.1 seconds左右
读取进程的历史最佳状态: 105.9 seconds左右 (包括0.2 seconds index构建)
Range进程的历史最佳状态: 192.1 seconds左右 (包括0.2 seconds index构建)
```

* 进程启动间隔

```
写入启动的间隔: 0.1 seconds 左右
写入到读取的间隔: 0.35 seconds左右
读取到Range的间隔: 0.45 seconds左右
```
