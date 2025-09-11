#!/bin/bash

TZ=Asia/Shanghai
time=$(date +%Y%m%d-%H%M%S)
log=/var/log/vmip.log
script_file=$(basename $0)
tg_api=
tg_id="
"
expire_day=0

#生产
node_ip=10.190.0.13
region=asia-south2
base_name=py-india-prod-card
new_name=$base_name

#测试
#node_ip=10.160.0.4
#region=asia-south1
#base_name=py-india-test
#new_name=$base_name

urls='
  https://qq.com
'
ua="User-Agent: Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Mobile Safari/537.36"



chk_conn(){
  for url in $urls ;do
    code=$(ssh root@$node_ip "curl --retry 6 -X POST -H \"$ua\" \"$url\" -o /dev/null -sw \"%{http_code}\"")
    if [ $code = 000 ] ;then
      echo err $url
      return 1
    fi
  done
  echo ok none
}
now_time(){
  date +"%F %T"
}
seched(){
  job_num=$(ps -ef |grep -v grep |grep -o $script_file |wc -l)
  #终端运行为2，cron运行为3，因为开了多个子shell
  if [ $job_num -eq 2 ] ;then
     echo "`now_time` 检测到无任务，开始执行"
  elif [ $job_num -le 3 ] ;then
     echo "`now_time` 检测到无任务，开始执行"
  else
    echo "`now_time` 检测到有任务，跳过执行"
    return 1
  fi
}
update_ip(){
  now_inet_name=$1
  new_inet_name=$2
  #通过vpc静态ip获取获取详细信息
  base=`gcloud compute addresses describe $now_inet_name --region=$region |grep -A 1 users: |grep ^-`
  #获取实例名
  vm_name=${base##*/}
  #区域
  zone=${base##*zones/}
  zone=${zone%%/*}
  #获取虚拟机网卡详细配置
  base=`gcloud compute instances describe $vm_name --zone $zone |grep -A 5 accessConfigs |grep name:`
  #网卡配置名
  vm_net_name=${base##*: }
  
  echo "`now_time` 获取信息: $vm_name $zone $vm_net_name"
  #预留ip
  gcloud compute addresses create $new_inet_name \
         --region=$region
  export ip=`gcloud compute addresses list |grep ^$new_inet_name |awk '{print $2}'`
  echo "`now_time` 预留ip完成: $ip"
  echo $ip > $log

  #删除网卡配置
  gcloud compute instances delete-access-config $vm_name --zone $zone \
    --access-config-name="$vm_net_name"
  echo "`now_time` 删除网卡配置完成"
  gcloud compute instances add-access-config $vm_name \
    --access-config-name="$vm_net_name" --address=$ip --zone $zone
  echo "`now_time` 网卡配置完成"
}
release_ip(){
  expire_date=`date +%Y%m%d -d "$expire_day days ago"`
  if [ $expire_day = 0 ] ;then
    expire_date=$((expire_date+1))
  fi
  release_name=`gcloud compute addresses list |grep RESERVED |grep -v default-ip-range |awk '/py/{print $1}'`
  for i in $release_name ;do
    ip_date=`echo $i |sed -nr "s@.*-([0-9]{8})-.*@\1@p"`
    if [ -z $ip_date ] ;then
      tag=1
    elif [ $ip_date -le $expire_date ] ;then
      tag=1
    fi
    if [ $tag = 1 ] ;then
      yes |gcloud compute addresses delete $i --region=$region
    fi
    tag=0
  done
}
tg_notify(){
  msg=$1
  for id in $tg_id ;do
    curl -s -X POST https://api.telegram.org/bot$tg_api/sendMessage?chat_id=$id \
      -H 'Content-Type: application/json' \
      -d "{\"msgtype\": \"text\",
         \"text\":\"$msg\"
         }"
  done
  echo
}
main(){
  now_inet_name=$(gcloud compute addresses list --regions $region |grep $base_name |awk '{print $1}' |sort |tail -n1)
  new_inet_name=$new_name-$time
  if [ ! -z "$now_inet_name" ] ;then
    echo "`now_time` 开始更换ip, 当前eip设备: $now_inet_name ,预新建eip设备: $new_inet_name"
    update_ip $now_inet_name $new_inet_name
    echo "`now_time` 更换ip完成"
    tg_notify "最新ip: $ip"
  else
    echo "`now_time` 未获取到绑定eip名称"
    return 1
  fi
}


case $1 in
  release)
    release_ip
    ;;
  manual)
    seched || exit 1
    main
    ;;
  *)
    seched || exit 1
    base_rep=`chk_conn`
    rep_code=$(echo $base_rep |awk '{print $1}')
    rep_url=$(echo $base_rep |awk '{print $2}')
    if [ $rep_code = 'err' ] ;then
      echo > /var/log/uip.log
      echo "`now_time` 请求不通，连接: $rep_code, url: $rep_url"

      main
    else
      echo "`now_time` 请求正常，连接: $rep_code, url: $rep_url"
    fi
    ;;
esac
