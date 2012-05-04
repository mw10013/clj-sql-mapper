# clj-sql-mapper

Dynamic SQL for Clojure with apologies to [mybatis](http://mybatis.org/).
Based on the [Dynamic SQL](http://www.mybatis.org/core/dynamic-sql.html) work in mybatis.
No actual sql mapper functionality yet.

## Getting started

Simply add clj-sql-mapper as a dependency to your lein project:

```clojure
[org.clojars.mw10013/clj-sql-mapper "0.0.2"]
```

## Usage

The namespace for dynamic SQL functions is clj-sql-mapper.sql.

    => (require '(clj-sql-mapper [sql :as sql]))

Compile SQL:

    => (def sql (sql/sql "select * from table where title = :title"))

Prepare it with a parameter map:

    => (sql/prepare {:title "the-title"} sql)
    ["select * from table where title = ?" "the-title"]

Conditionally include a part of a where clause:

    => (sql/prepare {:title "the-title"} (sql/sql "select * from blog where state = 'ACTIVE'"
         (sql/when :title "and title like :title")))
    ["select * from blog where state = 'ACTIVE'and title like ?" "the-title"]    

sql/when takes a predicate of one arg, a parameter map:

    => (sql/prepare {:title "the-title"} (sql/sql "select * from blog where state = 'ACTIVE'"
         (sql/when #(:title %) "and title like :title")))
    ["select * from blog where state = 'ACTIVE'and title like ?" "the-title"]

sql/where trims the first and/or in the where clause:

    => (sql/prepare {:title "the-title"} (sql/sql (sql/where (sql/when :author "and author = :author")
                                                             (sql/when :title "and title = :title"))))
    [" where title = ?" "the-title"]

sql/set trims the last comma:

    => (sql/prepare {:title "the-title" :author "clinton"}
                      (sql/sql (sql/set (sql/when :title "title = :title,")
                                        (sql/when :author "author = :author,"))))
    [" set title = ?, author = ?" "the-title" "clinton"]

Using vars:

    => (def cols (sql/sql "col1, col2, col3"))
    => (def title (sql/sql "and title = :title"))
    => (sql/prepare {:title "the-title"} (sql/sql "select #'cols from table" (sql/where #'title)))
    ["select col1, col2, col3 from table where title = ?" "the-title"]

sql/cond chooses the first non-empty sql string:

    => (sql/prepare {} (sql/sql (sql/cond (sql/when :title "title = :title")
                                          (sql/when :author "author = :author")
                                          "otherwise")))
    ["otherwise"]

sql/coll builds a collection:

    => (sql/prepare {:coll ["a" "b" "c"]} (sql/sql (sql/coll :coll)))
    [" ('a', 'b', 'c')"]

## License

Copyright (C) 2012 Michael Wu

Distributed under the Eclipse Public License, the same as Clojure.

