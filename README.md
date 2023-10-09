## 项目说明

使用 poetry 作为依赖管理, conda 作为虚拟环境管理 

* pycharm 中 poetry environment 创建虚拟环境, 由于系统非英文编码, 导致创建失败, 故使用 conda 作为虚拟环境管理, 作者使用 python 3.9 作为开发环境.

* 如果按照 poetry 官方手册进行操作, 创建了多余的 venv, 删除即可. 请检查如下位置 (注意隐藏路径):

    > C:\Users\fangy\AppData\Local\pypoetry\Cache\virtualenvs


* 如果没有自动切换虚拟环境, 手动命令切换:
    ```
    $ conda activate your_venv_name
    ```
    切换到虚拟环境后, 命令行执行 `poetry update/init/install/add` 等命令, 可以参考该 [stack overflow 链接](https://stackoverflow.com/questions/70851048/does-it-make-sense-to-use-conda-poetry)