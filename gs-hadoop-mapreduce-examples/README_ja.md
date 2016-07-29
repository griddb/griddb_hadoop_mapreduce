GridDBコネクタのサンプルプログラム

## 概要

exec-example.shでGridDBコネクタを用いた以下のサンプルプログラムを実行することができます。

    wordcount:  GridDBに格納されたテキストデータ中の単語の出現回数を計数します

exec-example.shでは以下の手順で処理を実行します。

    (1) MapReduceの入出力対象となるGridDBのコンテナを指定された個数作成します
    (2) 指定されたテキストファイル（複数指定可）の内容を行単位で入力用コンテナに読み込みます
    (3) wordcountのMapReduce処理を実行し、出力用コンテナに結果を格納します
    (4) 出力用コンテナに格納された実行結果を読み出し、標準出力に出力します
    (5) サンプルプログラムのために作成した入出力用コンテナを削除します

入力用コンテナと出力用コンテナの名前は以下のとおりです。

    入力用コンテナ：input_1 ～ input_N (Nのデフォルト値は2で、引数で変更可能）
    出力用コンテナ：output

GridDBの接続情報として以下のプロパティを指定できます。

    --define notificationAddress=<notification address> 任意(デフォルトは239.0.0.1)
    --define notificationPort=<notification port>       任意(デフォルトは31999)
    --define clusterName=<cluster name>                 必須
    --define user=<user name>                           必須
    --define password=<password>                        必須

### wordcount

wordcountを実行するためのexec-example.shの仕様は以下のとおりです。

    ./exec-example.sh   --job wordcount
            [--num-containers <number of input containers>]
            --define <key=value> ...
            <filename> ...

    <filename>で指定されたテキストファイル中の単語の出現回数を計数します。
