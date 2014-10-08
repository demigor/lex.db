##Lex.DB

Lex.DB is a lightweight, superfast, in-process database engine, completely written in AnyCPU C#. 

###Why?
We feel the need in small, fast and platform-neutral solution to store and access data locally. SQLite is almost good, but it is binary platform-specific (x32/x64/ARM versions of SQLite.dll), and has no real support for Silverlight. 

###Supported platforms:

* New: Xamarin.iOS & Xamarin.Android.
* New: Universal Windows Store Apps Support
* New: PCL version for supported platforms 
* .NET 4.0+, 
* Silverlight 5.0+, 
* Windows Phone 8.0+, 
* WinRT+.

Write your data access layer once, run everywhere (x64, x32, ARM) without recompilation.

Lex.DB supports concurrent database access, so multiple instances of your application are safe to go (.NET, Silverlight). Lex.DB also provides both synchronous and asynchronous database interface to avoid UI blockage.

Usage is greatly inspired by [Sterling](http://sterling.codeplex.com/), but performance is faster than native SQLite.

[Lex.DB.Sync](https://github.com/demigor/lex.db.sync) - lightweight data synchronization framework is logical extensions of Lex.DB.


####Features still in development
* Serialization of complex types
* Serialization of references & lists of references (one-to-many, many-to-many associations)
* Single file schema (right now each table has two files: index and data)


Check [author blog ](http://lexblog.azurewebsites.net)for more information about Lex.DB.

