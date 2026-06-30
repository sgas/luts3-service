#!/bin/sh

# This script will update all certs and CRLs into a single file with certs and
# CRLs, which can be fed to nginx. Nginx is then restarted to read the new
# certs and CRLs.
#
# fetch-crl should be run in cron, in order to get updates CRLs regularly.
# Note: If CRLs older than 30 days will often cause problems.
#
# The following options should be set in the SGAS nginx config file:
#
#    ssl_client_certificate /etc/grid-security/igtf-bundle.pem;
#    ssl_crl                /etc/grid-security/igtf-bundle-crl.pem;
#
# Original script contributed Ulf Tigerstedt

cat /etc/grid-security/certificates/*.0 > /etc/grid-security/igtf-bundle.pem
cat /etc/grid-security/certificates/*.r0 > /etc/grid-security/igtf-bundle-crl.pem

/etc/init.d/nginx restart 2>&1 /dev/null

