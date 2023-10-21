gulf
========================
<div align="center">
<img src="https://github.com/FangyangJz/gulf/assets/19723117/2d2a06b7-e4f5-429a-b87a-1b850070d033?raw=true" width="50%">
</div>

[English](README.md)

## 项目描述

**`gulf`** 是一款开源的集成金融数据源 [akshare](https://github.com/akfamily/akshare) 和数据库 [DolphinDB](https://www.dolphindb.com/) 的应用

## 项目说明

### 安装

最新版本安装:

```
pip install gulf
```

如果要修改 `gulf`, 或者使用最新功能, 可以使用 `pip` 的编辑模式安装:

```bash
git clone https://github.com/FangyangJz/gulf.git
pip install -e .
```

### 环境配置

使用 `poetry` 作为依赖管理, `conda` 作为虚拟环境管理 

* `PyCharm` 中 `poetry` venv 创建虚拟环境, 由于操作系统非原生英文, 导致创建失败, 故使用 `conda` 作为虚拟环境管理, 作者使用 `python` 3.9 作为开发环境.

* 如果按照 `poetry` 官方手册进行操作, 创建了多余的 venv, 删除即可. 请检查如下位置 (注意隐藏路径):

    > C:\Users\fangy\AppData\Local\pypoetry\Cache\virtualenvs


* 如果没有自动切换虚拟环境, 手动命令切换:
    ```
    $ conda activate your_venv_name
    ```
    切换到虚拟环境后, 命令行执行 `poetry update/init/install/add` 等命令, 可以参考该 [stack overflow 链接](https://stackoverflow.com/questions/70851048/does-it-make-sense-to-use-conda-poetry)

### DolphinDB 

* [前往官网下载客户端](https://www.dolphindb.cn/)
* [Dolphindb 使用文档链接](https://docs.dolphindb.cn/zh/help/index.html)
* [Python API 文档链接](https://gitee.com/dolphindb/api_python3/blob/master/README_CN.md)
* [视频教程链接](https://space.bilibili.com/1351925320)

>关于 .dos 文件的执行, 推荐使用 DolphinDB vscode 插件, 无需使用官方 GUI 程序