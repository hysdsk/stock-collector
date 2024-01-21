# stock-collector
日本株の情報を収集する。

| 単位 | 内容 |提供元 |
| ---- | ---- | ---- |
| 日次 | 株価 | [SoftHompo](http://softhompo.a.la9.jp/index.html) |
| 日次 | 融資貸株残高 | [日本証券金融株式会社](https://www.taisyaku.jp/) |
| 週次 | 信用残高 | [SoftHompo](http://softhompo.a.la9.jp/index.html) |
| 週次 | 貸付残高 | [日本証券業協会](https://www.jsda.or.jp/shiryoshitsu/toukei/kabu-taiw/index.html) |

## 設定

| Section | Key | Value |
| ---- | ---- | ---- |
| resources | taisyaku_dire_path | 貸借ディレクトリ名 |
| resources | softhompo_kabuka_dire_path | 株価ディレクトリ名 |
| resources | softhompo_shinyo_dire_path | 信用ディレクトリ名 |
| resources | jsda_newdeal_dire_path | 新規貸株ディレクトリ名 |
| resources | jsda_balance_dire_path | 貸株残高ディレクトリ名 |
| resources | exclude_days_filename | 除外日ファイル名 |
| database | db_host | DBホスト名 |
| database | db_user | DBユーザ名 |
| database | db_pswd | DBパスワード |
| database | db_name | DB名 |
