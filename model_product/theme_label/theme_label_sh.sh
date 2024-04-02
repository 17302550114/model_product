#!/usr/bin/env bash
 
echo "开始执行标签专题库脚本......"
cd /home/zhxz_model/ && /root/anaconda3/bin/python3 model_product/theme_label/theme_label_lgtz.py
cd /home/zhxz_model/ && /root/anaconda3/bin/python3 model_product/theme_label/theme_label_zfyc.py
cd /home/zhxz_model/ && /root/anaconda3/bin/python3 model_product/theme_label/theme_label_rxtx.py
cd /home/zhxz_model/ && /root/anaconda3/bin/python3 model_product/theme_label/theme_label_pfrz.py
cd /home/zhxz_model/ && /root/anaconda3/bin/python3 model_product/theme_label/theme_label_pfsw.py
cd /home/zhxz_model/ && /root/anaconda3/bin/python3 model_product/theme_label/theme_label_jxcx.py
echo "结束执行......"
