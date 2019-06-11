(defpackage #:ragno/crawler
  (:use #:cl)
  (:import-from #:ragno/spider
                #:spider
                #:scrape)
  (:import-from #:ragno/http
                #:*user-agent*)
  (:import-from #:ragno/errors
                #:ragno-concurrency-limit
                #:retry-after)
  (:import-from #:psychiq
                #:worker
                #:perform
                #:enqueue-in-sec)
  (:import-from #:vom)
  (:export #:crawler
           #:crawler-depth
           #:crawler-user-agent))
(in-package #:ragno/crawler)

(defclass crawler (spider psy:worker)
  ((depth :initarg :depth
          :initform -1
          :accessor crawler-depth)
   (user-agent :initarg :user-agent
               :initform "Ragno-Crawler"
               :accessor crawler-user-agent)))

(defmethod psy:perform ((crawler crawler) &rest args)
  (let ((uri (first args))
        (depth (second args))
        (*user-agent* (crawler-user-agent crawler)))
    (setf (crawler-depth crawler) depth)
    (handler-case
        (scrape crawler uri)
      (ragno-concurrency-limit (e)
        (vom:info "Retry ~S after ~S secs"
                  (class-name (class-of crawler))
                  (retry-after e))
        (psy:enqueue-in-sec (retry-after e)
                            (class-name (class-of crawler))
                            args)))))
