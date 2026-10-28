-- Tests drop and recreate their schema on every run, so they get their own database
-- and can never touch data in `dialer`.
CREATE DATABASE dialer_test OWNER dialer;
