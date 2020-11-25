---
title: "Rebuilding Redis in Ruby"
date: 2020-05-23T10:27:27-04:00
lastmod: 2020-05-23T10:27:27-04:00
draft: false
keywords: []
description: "Introduction to Redis in Ruby and who the book is for"
tags: []
categories: []
---

# Welcome!

This website is a free online book about rebuilding [Redis™*](https://redis.io/) in Ruby, from scratch. It is still a work in progress, the first eleven chapters are currently available, and you can see the planned table of content on the [chapters page](/chapters).

Start reading the first [chapter below](#posts), or head to the [chapters list](/chapters/)

## Who is this for?

This online book is aimed at people with some programming experience. I am assuming a reasonable level of familiarity with programming concepts such as conditions, loops, classes as well as some understanding of networking. Readers should already have heard of protocols such as [HTTP][http] and [TCP][tcp] and I think it would help to have already worked with those.
Anyone who worked with a web application, regardless of the language, should have enough experience to read this book.
If you've worked with a server backend in Ruby, Python, Javascript, or literally anything else, or did frontend work involving fetch and/or ajax, this should be enough!

Lastly, readers should also be familiar with [threads][wikipedia-threads] and [processes][wikipedia-processes]. No expertise is required, and I am far from being an expert on the topic, but if you've never heard of these, I would advise to glance at the linked wikipedia pages, and to potentially explore the APIs of your favorite languages. Most languages provide tools to interact with threads and processes. Ruby, which we'll be using in this book, has [a Thread class][ruby-doc-thread] and [a Process class][ruby-doc-process].

I am writing this book aiming for it to be useful to five years ago me, when I had about 2 and 3 years of professional experience. I majored in computer science, so back then I had already a few years of experience with programming. That being said, a degree in CS is definitely not required to read this.

On a spectrum from beginner to expert, I would say that this book lands somewhere in the middle, but leaning towards beginner, slightly to the left, so intermediate-ish.


[http]:https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol
[tcp]:https://en.wikipedia.org/wiki/Transmission_Control_Protocol
[wikipedia-threads]:https://en.wikipedia.org/wiki/Thread_(computing)
[wikipedia-processes]:https://en.wikipedia.org/wiki/Process_(computing)
[ruby-doc-thread]:https://ruby-doc.org/core-2.7.1/Thread.html
[ruby-doc-process]:https://ruby-doc.org/core-2.7.1/Process.html

_\* Redis is a trademark of Redis Labs Ltd. Any rights therein are reserved to Redis Labs Ltd. Any use by this website is for referential purposes only and does not indicate any sponsorship, endorsement or affiliation between Redis and this website_
