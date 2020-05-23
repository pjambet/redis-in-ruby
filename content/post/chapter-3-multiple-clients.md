---
title: "Chapter 3 - Multiple Clients"
date: 2020-05-18T01:30:32-04:00
lastmod: 2020-05-18T01:30:32-04:00
draft: true
keywords: []
description: "In this chapter we will improve the Redis server to efficiently handle multiple clients connected at the same time"
---

## Introduction
👋

## First problem, accepting clients

Let's start with the new client problem. The goal is that, regardless of the state of the server, of what it may or may
not currently doing, or whether other clients are already connected, new clients should be able to establish a new
connection, and keep the connection open as long as they wish, until they either disconnect on purpose or a network
issue occurs.
