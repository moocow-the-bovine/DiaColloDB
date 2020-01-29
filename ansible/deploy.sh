#!/bin/bash

dir0=$(dirname "$0")
set -o xtrace
exec ansible-playbook -i "$dir0"/ansible.hosts -K "$dir0"/dstar-deploy-playbook.yml
