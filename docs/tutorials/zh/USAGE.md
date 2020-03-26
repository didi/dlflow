# 使用说明

## 运行方式

DLFlow支持多种运行方式，这里推荐使用内建的Pipeline来运行程序，当安装DLFlow并写好配置文件后，就可以按照如下的方式启动DLFlow。

### 以模块方式运行

```bash
python -m dlflow.main --config <CONFIGURATION FILE>.conf
```

### 以脚本方式运行

首选确保python环境的bin目录已经被添加到环境变量中
```bash
export PATH=$PATH:/usr/local/python/bin
```

之后就可以
```
dlflow --config <CONFIGURATION FILE>.conf
```

## 运行参数

### 必选参数

**配置文件**

```
dlflow --config <CONFIGURATION FILE>.conf
```

配置文件是DLFlow运行的关键信息，其中需要设置与任务运行相关的多个配置信息。详细的信息参考xxxx。

### 可选参数

**日志写入外部文件**

```
dlflow --log_dir <LOG DIR> --config <CONFIGURATION FILE>.conf
```

默认情况下日志不写入外部，设置`--log_dir`后，日志将全部写入指定目录中。

**日志等级**

```
dlflow --log_level <LOG LEVEL> --config <CONFIGURATION FILE>.conf
```

日志等级分为四级：

 * debug
 * info
 * warn
 * error

**输出语言**

```
dlflow --lang <LANGUAGE> --config <CONFIGURATION FILE>.conf
```

目前DLFlow支持两种语言：

* zh - 中文
* en - 英文

默认情况下会根据环境变量`$LANG`去读取用户语言，如果没设置该环境变量，则使用英语。

**运行时配置**

```
dlflow --config <CONFIGURATION FILE>.conf --conf "<KEY_1>=<VALUE_1>;<KEY_2>=<VALUE_2>"
```

```
dlflow --config <CONFIGURATION FILE>.conf --conf "<KEY_1>=<VALUE_1>" --conf "<KEY_2>=<VALUE_2>"
```

运行时配置的作用同配置文件中配置相同，其优先级高于配置文件。因此，运行时配置可用于修改配置文件中的任何配置项，也可用于设置配文件中没有给出的配置项。
