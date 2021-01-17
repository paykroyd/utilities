(define (square n) (* n n))

;; uses newton's method to improve the guess of the sqrt
;; by averaging the guess with x/guess
(define (improve-guess guess x)
    (/ (+ guess (/ x guess)) 2))

(define (close-enough? guess x)
    (< (abs (- (square guess) x)) .001))

(define (sqrt-iter guess x)
    (if (close-enough? guess x) guess
        (sqrt-iter (improve-guess guess x) x)))

(define (sqrt-newton-method x)
    (sqrt-iter 1.0 x))