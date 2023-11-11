gulf
========================
<div align="center">
<img src="https://github.com/FangyangJz/gulf/assets/19723117/2d2a06b7-e4f5-429a-b87a-1b850070d033?raw=true">
</div>

[简体中文](README_CN.md)

## Description

**`gulf`** is an open-source integrated finance data source and database application on top of [DolphinDB](https://www.dolphindb.com/) and [akshare](https://github.com/akfamily/akshare).

### Installation

You can install the latest stable version from pip using:
```
pip install gulf
```
If you plan to develop gulf yourself, or want to be on the cutting edge, you can use an editable install:
```bash
git clone https://github.com/FangyangJz/gulf.git
pip install -e .
```

### About development environment

Use `poetry` as dependency management and `conda` as virtual environment management.

* In `PyCharm` settings, `poetry` venv creation may meet error, because the OS system is not native English. So `conda` was used as the virtual environment management, and author used `python` 3.9 version.

* If you follow the official `poetry` manual and create a redundant venv, just delete it. Check the following locations (note hidden paths):

    > C:\Users\fangy\AppData\Local\pypoetry\Cache\virtualenvs


* If the virtual environment is not automatically activated, manually switch:
    ```
    conda activate your_venv_name
    ```
    After activate the virtual environment, `poetry update/init/install/add` command is executed on the shell. [Stack overflow link](https://stackoverflow.com/questions/70851048/does-it-make-sense-to-use-conda-poetry)

### DolphinDB 

* [Official website download client](https://www.dolphindb.cn/)
* [Dolphindb document](https://docs.dolphindb.cn/zh/help/index.html)
* [Python API document](https://gitee.com/dolphindb/api_python3/blob/master/README_CN.md)
* [Video tutorial link](https://space.bilibili.com/1351925320)
* [Official Q&A community](https://ask.dolphindb.net/)

> For .dos file execution, recommend using the DolphinDB vscode plug-in instead of the official GUI program.

