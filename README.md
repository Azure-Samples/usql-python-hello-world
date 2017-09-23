---
services: data-lake-analytics
platforms: python
author: saveenr
---

# U-SQL/Python Hello World

```
REFERENCE ASSEMBLY [ExtPython];

DECLARE @myScript = @"
def get_mentions(tweet):
    return ';'.join( ( w[1:] for w in tweet.split() if w[0]=='@' ) )

def usqlml_main(df):
    del df['time']
    del df['author']
    df['mentions'] = df.tweet.apply(get_mentions)
    del df['tweet']
    return df
";

@t  = 
    SELECT * FROM 
        (VALUES
            ("D1","T1","A1","@foo Hello World @bar"),
            ("D2","T2","A2","@baz Hello World @beer")
        ) AS 
              D( date, time, author, tweet );

@m  =
    REDUCE @t ON date
    PRODUCE date string, mentions string
    USING new Extension.Python.Reducer(pyScript:@myScript);


OUTPUT @m
  TO "/tweetmentions.csv"
  USING Outputters.Csv();
```

