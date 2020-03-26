#!/bin/bash

SOURCE_DIR="./dlflow"
MSG_PO="./dlflow/resources/locale/zh/LC_MESSAGES/msg.po"
MSG_MO="./dlflow/resources/locale/zh/LC_MESSAGES/msg.mo"
SOURCE_PO=${MSG_PO}

cd ../
python3 ./tools/i18n_extract.py \
            -i ${SOURCE_DIR} \
            -m ${SOURCE_PO} \
            -o ${MSG_PO}

msgfmt -o ${MSG_MO} ${MSG_PO}
