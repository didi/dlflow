# 配置项结构

FeatureFlow使用 [HOCON](https://github.com/lightbend/config/blob/master/HOCON.md) 格式，主要由三部分构成：

* **settings**：用于兜底的全局默认配置项。若特征处理流程中的 *operator* 未设置个性化子项，则默认会使用全局配置项。同时，也可以通过设置 `enableOverride=true` 强制用全局配置覆盖个性化子项配置。
* **dependencies**：用于解析上游数据依赖的配置项。在特征产出运行时，会根据执行时刻的特征基准日期，按照 *dependencies* 中设置的数据源检查条件，获取到可用的数据源。可用数据源将组成 `K-V pairs`，用于替换 
*operator* 中的占位符
* **steps**：用于设置特征合并的流程。目前支持的 *operator* 有 `SQL_Assembler`、`UDF_Assembler`、`MissingValueCounter`、`MissingValueFiller`

一个典型配置项可参考：[Config](example/FeatureFlow.conf)

---

# 配置项说明

## 1. 通用子配置项

### (1) checkpoint

用于指定处理结果是否需要落盘HDFS, 示例如下：

```hocon
checkpoint: {             
    enable: true,
    overwrite: false,       
    clear: true
  }
```

配置项说明

| 字段 | 选项 | 默认值 | 说明 |
|:-------------|:-------------:|:-------------:|:-------------|
| enable | optional | false | 是否开启checkpoint功能，开启会存储到HDFS |
| overwrite | optional | false | 是否强制覆盖已有的checkpoint，开启时checkpoint实际上失效，目的是为了强制拆分spark的DAG |
| clear | optional | true | 执行完毕后，是否清理checkpoint |

*备注*：目前的 checkpoint 默认使用 `parquet` 格式落盘，同时会`强制检测`落盘数据是否为空


### (2) checker

用于指定数据源的检查方式，目前支持3种检查方式：`_SUCCESS`，`capacity`， `count` 

**_SUCCESS**

按是否产出 _SUCCESS 文件

```hocon
checker: {
  method: _SUCCESS
}
```

**capacity**

按目录的大小是否满足最小阈值（此种方式用于目标数据源不产出 _SUCCESS 文件的情况）

```hocon
checker: {
  method: capacity,
  threshold: 1000.1  
}
```

*备注*：threshold 的单位是 `MB`

**count**

按数据源记录调试是否满足最小阈值（此种方式适用于spark dataframe数据源）

```hocon
checker: {
    method: count,       
    threshold: 10000,      
    sourceType: parquet   
  }
```

*备注*：sourceType 可选值 `parquet`，`orc`，`json`

## 2. settings

全局默认配置项

```hocon
settings: {
  enableOverride: false,
  checkpoint: {},
  parallelism = 1000      
}
```

| 字段 | 选项 | 默认值 | 说明 |
|:-------------|:-------------:|:-------------:|:-------------|
| enableOverride | optional | false | 是否用settings的配置项覆盖operator中的个性化子配置项，针对checkpoint有效 |
| checkpoint | optional |  | checkpoint配置项，详见：通用子配置项 |
| parallelism | optional | 1000 | dataframe 执行 join 操作时的并行度 |

## 3. dependencies

上游数据依赖配置

在特征合并过程中，将结合运行时的`特征日期`，依据此配置项获取到实际可用的数据源信息，并组成 `K-V pairs` 供合并过程各*step*做占位符替换

主要包括4种检测类型：

* **hive**：通过hive表分区地址，获取到可用的hive分区对应的时间字段
* **hdfs**: 通过地址模板，获取可用的HDFS地址（一般是 dataframe 数据源）
* **date**：直接返回特征运行日期相应偏移后的日期（常用于选取label或者是取历史范围的数据）
* **static**：一些静态变量，直接用于占位符替换（常用于 sql 中的 where 条件）

典型的配置结构如下：

```hocon
dependencies: {
  hive: [],
  hdfs: [],
  date: [],
  static: []
}
```

备注：各依赖类型，没有的可以不填

### hive

hive类型的依赖，对依赖的hive表地址进行检测后，最终是以 `日期字符串` 格式返回（格式由 *formatter* 决定）

```hocon
  hive: [
    {
      key: HIVE_COMPACT_DATE  
      formatter: yyyyMMdd    
      offsetDays: -1,         
      degradeDays: 3,         
      pathTemplate: "/YOUR_HDFS_PREFIX/warehouse.db/hive/YOUR_TABLE/${YYYY}/${MM}/${DD}",
      checker: {
        method: capacity,
        threshold: 1000.1  
      }
    }
  ]
```

| 字段 | 选项 | 默认值 | 说明 |
|:-------------|:-------------:|:-------------:|:-------------|
| key | required |  | 数据源最终返回 K-V pairs 时对应的key，供占位符替换使用 |
| formatter | required |  | 最终返回的日期字符串格式，可选值：yyyyMMdd， yyyy-MM-dd，yyyy/MM/dd, yyyy, MM, dd |
| offsetDays | optional | 0 | 是否在特征日期基础上进行日级别的偏移，正数是往特征日期后偏移，负数是往前偏移 |
| degradeDays | optional | 0 | 是否需要降级，最多降级多少天？必须 >= 0 |
| pathTemplate | required | | 数据源检查的地址模板，会根据执行时刻的：特征日期 + offsetDays，确定真实的日期，并对地址模板进行替换 |
| checker | optional | _SUCCESS | 进行数据源检测时用的checker配置，详见：通用子配置项 |

### hdfs

hdfs类型的依赖，最终是以 pathTemplate 按检查通过的日期替换后的 `地址字符串`

```hocon
  hdfs: [
    {
      key: UPSTREAM_PARQUET
      offsetDays: 0,
      degradeDays: 3,
      pathTemplate: "/YOUR_HDFS_PREFIX/warehouse.db/hive/YOUR_TABLE/${YYYY}/${MM}/${DD}"
      checker: {
        method: _SUCCESS
      }
    }
  ]
```

| 字段 | 选项 | 默认值 | 说明 |
|:-------------|:-------------:|:-------------:|:-------------|
| key | required |  | 数据源最终返回 K-V pairs 时对应的key，供占位符替换使用 |
| offsetDays | optional | 0 | 是否在特征日期基础上进行日级别的偏移，正数是往特征日期后偏移，负数是往前偏移 |
| degradeDays | optional | 0 | 是否需要降级，最多降级多少天？必须 >= 0 |
| pathTemplate | required | | 数据源检查的地址模板，会根据执行时刻的：特征日期 + offsetDays，确定真实的日期，并对地址模板进行替换 |
| checker | optional | _SUCCESS | 进行数据源检测时用的checker配置，详见：通用子配置项 |

### date

date类型的依赖，最终是以 `日期字符串` 格式返回（格式由 formatter 决定）

```hocon
  date: [
    {
      key: YESTERDAY,
      formatter: yyyyMMdd,
      offsetDays: -1
    }
  ]
```

| 字段 | 选项 | 默认值 | 说明 |
|:-------------|:-------------:|:-------------:|:-------------|
| key | required |  | 数据源最终返回 K-V pairs 时对应的key，供占位符替换使用 |
| formatter | required |  | 最终返回的日期字符串格式，可选值：yyyyMMdd， yyyy-MM-dd，yyyy/MM/dd, yyyy, MM, dd |
| offsetDays | optional | 0 | 是否在特征日期基础上进行日级别的偏移，正数是往特征日期后偏移，负数是往前偏移 |

### static

静态占位符，直接用于替换

```hocon
  static: [
    {
      key: AGE,
      value: "20"
    }
  ]
```

| 字段 | 选项 | 默认值 | 说明 |
|:-------------|:-------------:|:-------------:|:-------------|
| key | required |  | 数据源最终返回 K-V pairs 时对应的key，供占位符替换使用 |
| value | required |  | K-V pairs 时对应的value，直接用于占位符替换，不做任何处理 |

## 4. steps

特征合并处理流程配置

上游数据依赖配置

用于决定特征合并的每一步具体如何执行，合并什么特征或是执行什么特征转化操作

主要包括4种类型的 *operator*：

* **SQL_Assembler**：通过sql模板提取特征，以指定的 joinKyes 合并到样本集，是最常用的 operator
* **UDF_Assembler**: 以自定义的UDF函数提取特征，以指定的 joinKyes 合并到样本集，主要用于特殊特征的生成
* **MissingValueCounter**：用于计算特征中缺失值的个数，将其作为新的特征填充到样本集中
* **MissingValueFiller**：缺失值填充 operator，可按列名或特征类型指定缺失值的填充方式

典型的配置结构如下：

```hocon
steps: [
  {
    name: "stepName",
    desc: "一个通过SQL模板合并特征的例子",
    opType: YOUR_OPERATOR_TYPE,   # operator类型，支持类型见上述描述
    checkpoint: {},
    params: {}                    # 该 operator 运行时需要的参数
  }
]
```

### SQL_Assembler

*opType*: SQL_Assembler

*用途*: 最常用的特征合并operator，通过编写SQL模板提取特征（可用运行时变量，对模板中的占位符进行替换），将提取到的特征合并到 `种子样本集` 上

```hocon
{
    name: "loadSQL",
    desc: "执行SQL文件获取特征进行合并",
    opType: SQL_Assembler,
    checkpoint: {},
    params: {
      joinKeys: [uid],
      sqlFile: "my_feature.sql",
      enableSubdir: false,
      emptyCheck: true,
      uniqueCheck: true,
      primaryKeys: [
        uid
      ],
      substitutions: [HIVE_COMPACT_DATE]
    }
  }
```

| 字段 | 选项 | 默认值 | 说明 |
|:-------------|:-------------:|:-------------:|:-------------|
| name | required |  | operator的标记，会打印到日志中 |
| desc | optional | 空 | operator的备注信息 |
| opType | required |  | 填写：SQL_Assembler |
| checkpoint | optional | 无 | 该step执行完毕后，中间结果是否要落盘，建议开启，详见：通用子配置项 |
| params.joinKeys | required |  | 样本集与SQL提取到的特征，按什么主键合并，该主键列必须在2边都存在 |
| params.sqlFile: | required |  | 提取特征使用的SQL模板文件路径 |
| params.enableSubdir: | optional | false | 执行SQL时是否设置：set spark.hadoop.hive.mapred.supports.subdirectories=true |
| params.emptyCheck: | optional | true | 执行SQL提取到的特征，是否做非空检查 |
| params.uniqueCheck: | optional | false | 执行SQL提取到的特征，是否做主键唯一性检查（需要配套 primaryKeys 一起使用） |
| params.primaryKeys: | optional | 空 | 做主键唯一性检查时，依据的主键是什么 |
| params.substitutions: | required | 空 | 依赖 `dependencies` 中的运行时占位符取值，用于替换SQL模板中的占位符（格式：`${XXX}`） |

备注：substitutions 无依赖的，传空list即可

### UDF_Assembler

*opType*: UDF_Assembler

*用途*: 可拓展的operator，用自定义的UDF函数提取特征，合并到 `种子样本集` 上

* **使用方式一**：使用dmflow中已实现的UDF（示例：*com.didi.dm.dmflow.feature.extractor.udf.HdfsReader*）
* **使用方式二**：继承 *com.didi.dm.dmflow.feature.extractor.IFeatureUDF* （提取特征方法：`invoke()`）

```hocon
{
    name: "myUDF",
    desc: "执行udf函数提取特征",
    opType: UDF_Assembler,
    checkpoint: {},
    params: {
      joinKeys: [pid],
      clazz: com.didi.dm.dmflow.feature.extractor.udf.HdfsReader,
      emptyCheck: true,
      uniqueCheck: true,
      primaryKeys: [
        uid
      ],
      extraParams: {    # 个性化参数根据具体实现确定
        path: "${UPSTREAM_PARQUET}",
        format: parquet,
      },
      substitutions: [UPSTREAM_PARQUET]
    }
  }
```

| 字段 | 选项 | 默认值 | 说明 |
|:-------------|:-------------:|:-------------:|:-------------|
| name | required |  | operator的标记，会打印到日志中 |
| desc | optional | 空 | operator的备注信息 |
| opType | required |  | 填写：UDF_Assembler |
| checkpoint | optional | 无 | 该step执行完毕后，中间结果是否要落盘，建议开启，详见：通用子配置项 |
| params.joinKeys | required |  | 样本集与UDF提取到的特征，按什么主键合并，该主键列必须在2边都存在 |
| params.clazz: | required |  | UDF反射依赖的包路径 |
| params.emptyCheck: | optional | true | 执行SQL提取到的特征，是否做非空检查 |
| params.uniqueCheck: | optional | false | 执行SQL提取到的特征，是否做主键唯一性检查（需要配套 primaryKeys 一起使用） |
| params.primaryKeys: | optional | 空 | 做主键唯一性检查时，依据的主键是什么 |
| params.extraParams: | required |  | UDF函数依赖的参数，Map[String, Any]，具体字段的约束请继承：`com.didi.dm.dmflow.feature.flow.param.BaseMapParams` |
| params.substitutions: | required | 空 | 依赖 `dependencies` 中的运行时占位符取值，用于替换 extraParams 中的占位符（格式：`${XXX}`） |

### MissingValueCounter

*opType*: MissingValueCounter

*用途*: 根据上一个operator产出的特征，计算所有特征中缺失值的个数及占比，将得到的统计量作为新的特征（除了全局统计，还会按特征类型分组统计）

```hocon
{
    name: "count feature",
    desc: "缺失特征值的统计量作为特征",
    opType: MissingValueCounter,
    checkpoint: {},
    params: {
      excludeCols: [uid, label]
    },
  }
```

| 字段 | 选项 | 默认值 | 说明 |
|:-------------|:-------------:|:-------------:|:-------------|
| name | required |  | operator的标记，会打印到日志中 |
| desc | optional | 空 | operator的备注信息 |
| opType | required |  | 填写：MissingValueCounter |
| checkpoint | optional | 无 | 该step执行完毕后，中间结果是否要落盘，建议开启，详见：通用子配置项 |
| params.excludeCols | required |  | 哪些列不参与缺失特征统计，主要是主键和label |

### MissingValueFiller

*opType*: MissingValueFiller

*用途*: 根据上一个operator产出的特征，对指定的列按指定的缺失值进行填充，也可以按字段类型指定填充

*备注*：按列表进行填充时，使用的是倒排的方式，即：key=待填充缺失值，value=需要填充的列名list

```hocon
{
    name: "fill missing value",
    desc: "填充缺失值",
    opType: MissingValueFiller,
    checkpoint: {},
    params: {
      excludeCols: [uid, label],
      int: {  
        -999: [my_feature_col_1, my_feature_col_2],
        default: 0
      },
      long: { 
        -999: [my_feature_col_3],
        default: 0
      },
      float: {
        default: 0.0
      },
      double: {
        default: 0.0
      },
      decimal: {
        default: 0.0
      },
      str: {
        "unknow": [my_feature_col_4, my_feature_col_5]
        default: "NULL"
      }
    },
  }
```

| 字段 | 选项 | 默认值 | 说明 |
|:-------------|:-------------:|:-------------:|:-------------|
| name | required |  | operator的标记，会打印到日志中 |
| desc | optional | 空 | operator的备注信息 |
| opType | required |  | 填写：MissingValueFiller |
| checkpoint | optional | 无 | 该step执行完毕后，中间结果是否要落盘，建议开启，详见：通用子配置项 |
| params.excludeCols | required |  | 哪些列不参与缺失特征统计，主要是主键和label |
| params.int | required |  | *int类型*字段的填充方式，default设置兜底缺失值，具体的 `Key-List` 对特定列按特定值填充 |
| params.long | required |  | *long类型*字段的填充方式，同上 |
| params.float | required |  | *float类型*字段的填充方式，同上 |
| params.double | required |  | *double类型*字段的填充方式，同上 |
| params.decimal | required |  | *decimal类型*字段的填充方式，同上 |
| params.str | required |  | *str类型*字段的填充方式，同上 |