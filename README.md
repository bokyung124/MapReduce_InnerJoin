## MapReduce Inner Join

**개발 환경**

||version|
|---|---|
|CPU|Apple M1 3.2GHz 8core|
|RAM|16GB|
|SSD|256GB 5400RPM|
|OS|macOS Sonoma 14.0|   
|Hadoop|3.3.6|  
|Spark|3.5.0|

<br>

<details>
<summary><strong>MapReduce 수행 시간</strong></summary>

|Data|Time|
|---|---|
|0.8GB|2분 56초|
|1.6GB|4분 39초|
|2.4GB|4분 1초|
|3.2GB|12분 15초|
|4.0GB|14분 41초|

</div>
</details>

<br>

<details>
<summary><strong>Spark 수행 시간</strong></summary>

|Data|Partition|Time|
|---|---|---|
|0.8GB|1|1분 18초|
|0.8GB|2|1분 17초|
|0.8GB|4|1분 10초|
|1.6GB|4|2분 5초|
|1.6GB|8|1분 52초|
|2.4GB|4|2분 56초|
|2.4GB|8|2분 42초|
|2.4GB|12|2분 33초|
|3.2GB|4|3분 46초|
|3.2GB|8|3분 40초|
|3.2GB|12|3분 15초|
|4.0GB|4|4분 29초|
|4.0GB|8|4분 20초|
|4.0GB|12|4분 8초|

</div>
</details>

