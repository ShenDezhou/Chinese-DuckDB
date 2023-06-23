# 中文DuckDB语料库

[DuckDB](https://github.com/duckdb/duckdb)是一个基于SQL的数据库管理系统(软件)，本文介绍如何在HuggingFace平台浏览中文数据集。

## HuggingFace数据集

在[datasets](https://huggingface.co/datasets?language=language:zh&sort=downloads)上截止当前，约有385个中文数据集。

以`clue`数据集为例， 执行`https://huggingface.co/datasets/{dataset-name}`


```shell
# Make sure you have git-lfs installed (https://git-lfs.com)
git lfs install
git clone https://huggingface.co/datasets/clue

# if you want to clone without large files – just their pointers
# prepend your git clone with the following env var:
#GIT_LFS_SKIP_SMUDGE=1
```

仓库的`dataset_infos.json`包含了子目录信息。

```text
dataset_name=clue
https://huggingface.co/datasets/{dataset_name}/resolve/main/dataset_infos.json
```

数据可视化为：
```json
{
  "afqmc": {
    "description": "CLUE, A Chinese Language Understanding Evaluation Benchmark\n(https://www.cluebenchmarks.com/) is a collection of resources for training,\nevaluating, and analyzing Chinese language understanding systems.\n\n",
    "citation": "\n@misc{xu2020clue,\n    title={CLUE: A Chinese Language Understanding Evaluation Benchmark},\n    author={Liang Xu and Xuanwei Zhang and Lu Li and Hai Hu and Chenjie Cao and Weitang Liu and Junyi Li and Yudong Li and Kai Sun and Yechen Xu and Yiming Cui and Cong Yu and Qianqian Dong and Yin Tian and Dian Yu and Bo Shi and Jun Zeng and Rongzhao Wang and Weijian Xie and Yanting Li and Yina Patterson and Zuoyu Tian and Yiwen Zhang and He Zhou and Shaoweihua Liu and Qipeng Zhao and Cong Yue and Xinrui Zhang and Zhengliang Yang and Zhenzhong Lan},\n    year={2020},\n    eprint={2004.05986},\n    archivePrefix={arXiv},\n    primaryClass={cs.CL}\n}\n",
    "homepage": "https://dc.cloud.alipay.com/index#/topic/data?id=8",
    "license": "",
    "features": {
      "sentence1": {
        "dtype": "string",
        "id": null,
        "_type": "Value"
      },
      "sentence2": {
        "dtype": "string",
        "id": null,
        "_type": "Value"
      },
      "label": {
        "num_classes": 2,
        "names": [
          "0",
          "1"
        ],
        "names_file": null,
        "id": null,
        "_type": "ClassLabel"
      },
      "idx": {
        "dtype": "int32",
        "id": null,
        "_type": "Value"
      }
    },
    "post_processed": null,
    "supervised_keys": null,
    "task_templates": null,
    "builder_name": "clue",
    "config_name": "afqmc",
    "version": {
      "version_str": "1.0.0",
      "description": "",
      "major": 1,
      "minor": 0,
      "patch": 0
    },
    "splits": {
      "test": {
        "name": "test",
        "num_bytes": 378726,
        "num_examples": 3861,
        "dataset_name": "clue"
      },
      "train": {
        "name": "train",
        "num_bytes": 3396535,
        "num_examples": 34334,
        "dataset_name": "clue"
      },
      "validation": {
        "name": "validation",
        "num_bytes": 426293,
        "num_examples": 4316,
        "dataset_name": "clue"
      }
    },
    "download_checksums": {
      "https://storage.googleapis.com/cluebenchmark/tasks/afqmc_public.zip": {
        "num_bytes": 1195044,
        "checksum": "5a4cb1556b833010c329fa2ad2207d9e98fc94071b7e474015e9dd7c385db4dc"
      }
    },
    "download_size": 1195044,
    "post_processing_size": null,
    "dataset_size": 4201554,
    "size_in_bytes": 5396598
  },
  "tnews": {
    
  }
}
```

上述信息下，获取了子目录的名字`afqmc,tnews`，对应的的parquet文件信息是：
```text
dataset_name=clue
working_dataset_name=[afqmc,tnews,...]
split=[train,test,validation]
https://huggingface.co/datasets/{dataset_name}/resolve/refs%2Fconvert%2Fparquet/{working_dataset_name}/{dataset_name}-{split}.parquet'
```


至此，我们就获得了clue的`len(working_dataset_name) * len(split)`全部parquet文件。

基于这些parquet文件，可使用duckdb库进行SQL查询。


## 查询数据库
函数名称：query_subdb
输入：数据库名
返回：可作为工作数据库的列表

函数名称：query_subdb_split
输入：数据库名、工作数据库名、样本类型(train\test\validation)
返回：数据库的parquet地址（URL格式）

函数名称：peek_parquet
输入：数据库parquet地址（URL格式）、SQL
返回：执行的SQL结果

示例SQL语句为:
```
SELECT count(*)
FROM 'local_parquet' 
LIMIT(10)
```

函数名：start
输入：void
返回：ping测试

函数名：stop
输入：void
返回：ping测试

## 示例

在parquet数据上利用duckdb执行SQL，如果我们想访问tnews/clue-test数据库，那么构造如下请求：

`POST http://transformers.science:33000/peek_parquet`

```json
{
  "url":"https://huggingface.co/datasets/clue/resolve/refs%2Fconvert%2Fparquet/tnews/clue-test.parquet",
  "sql":"SELECT count(*) FROM 'local_parquet' LIMIT(10)"
}
```

即可在parquet数据格式上，使用duckdb语法执行SQL语句，上述语句的结果为：

```json
{
    "data": [
        {
            "count_star()": 10000
        }
    ]
}
```

## 总结
本项目实现了在HF数据集上，利用DuckDB对数据集执行SQL语句的方法，从HF页面上获取parquet的URL后，执行peek_parquet函数进行SQL语句。

## 致谢

项目作者： Brian Shen. Twitter @dezhou.