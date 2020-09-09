# Assignment Big Data

Assignment sviluppato per il corso di Big Data. Si compone
di 2 Job sviluppati sia utilizzando MapReduce di Hadoop
che Spark.

I Job sono sviluppati utilizzando [questo](https://www.kaggle.com/ehallmar/beers-breweries-and-beer-reviews?select=reviews.csv)
dataset, che raccoglie birre, birrerie e recensioni, da parte di diverse persone, delle birre.

I due Job sono così definiti: 

* Top N birrerie (N passabile come parametro, default 20) con almeno N birre diverse (N secondo parametro, default 5)
  con le medie di voti piu alta.(N recensioni minime per ogni birra, terzo parametro, 50 default).
  
* Prima vengono classificate le birre in base alla media dei voti, poi vengono divise in classi
  di voto: media voto &lt;= 2 bassa qualita; 
  2&lt; media voto &lt;= 4 media qualita; media voto &gt; 4 alta qualità.
  Per ogni birreria viene calcolata la quantità di birre in ogni classe; alla fine, le birrerie
  vengono ordinate in base ad uno score, calcolato in base al numero di birre presente in ogni categoria, normalizzato 
  rispetto al massimo numero di ogni classe di birre assegnate alle birrerie.
  
Per eseguire i vari job è possibile utilizzare i seguenti comandi:

* Job1 Hadoop:
```bash
 hadoop jar progettoBD-1.0-SNAPSHOT.jar hadoop.job1.Job1 N1 N2 N3
```
I tre parametri sono tutti opzionali, con N1 = Numero birrerie da mettere in classifica finale;
N2 = minimo di birre per ogni birrerie per essere considerate nella classifica; N3 = minimo di recensioni per ogni birra
per essere considerate nella media dei voti.

Per vedere il risultato è possibile eseguire il comando: 
```bash
hdfs dfs -cat giovannim/dataset/output/datasetprogetto/hadoop/job1/*
```
* Job2 Hadoop:
```bash
 hadoop jar progettoBD-1.0-SNAPSHOT.jar hadoop.job2.Job2 N1 N2
```
I due parametri sono opzionali, con N1 = Numero birrerie da mettere in classifica finale;
N2 = minimo di recensioni per ogni birra per essere considerate nella media dei voti.

Per vedere il risultato è possibile eseguire il comando:

```bash
hdfs dfs -cat giovannim/dataset/output/datasetprogetto/hadoop/job2/*
```

* Job1 Spark:
```bash
spark2-submit --class spark.job1.Job1 progettoBD-1.0-SNAPSHOT.jar N1 N2 N3
```
I tre parametri sono tutti opzionali, con N1 = Numero birrerie da mettere in classifica finale;
N2 = minimo di birre per ogni birrerie per essere considerate nella classifica; N3 = minimo di recensioni per ogni birra
per essere considerate nella media dei voti.

Per vedere il risultato è possibile eseguire il comando:
```bash
hdfs dfs -cat giovannim/dataset/output/datasetprogetto/spark/job1/*
```
* Job2 Spark:
```bash
spark2-submit --class spark.job2.Job2 progettoBD-1.0-SNAPSHOT.jar N1 N2
```
I due parametri sono opzionali, con N1 = Numero birrerie da mettere in classifica finale;
N2 = minimo di recensioni per ogni birra per essere considerate nella media dei voti.

Per vedere il risultato è possibile eseguire il comando:
```bash
hdfs dfs -cat giovannim/dataset/output/datasetprogetto/spark/job2/*
```
