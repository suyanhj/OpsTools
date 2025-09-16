import subprocess
import threading
import logging
import fabric
import argparse
import os
import yagmail
from qiniu import Auth, DomainManager
import re


class DomainMgr(DomainManager):
    def list_domains(self, limit=100):
        req = {
            "limit": limit
        }
        url = '{}/domain'.format(self.server)

        return self._DomainManager__get(url, req)


class MyMail:
    def __init__(self, sender=None, sender_pw=None,
                 receiver=None, subject=None):
        """初始化邮件发送者的凭证和收件人信息

        参数:
            sender (str, optional): 发件人邮箱地址. 默认为 zachgonnahappyhappy@gmail.com
            sender_pw (str, optional): 发件人邮箱密码. 默认为应用专用密码
            receiver (set, optional): 收件人邮箱地址列表. 默认为 ["1137127273@qq.com"]
        """
        self.data = []
        self.mark = False
        self.sender = sender or "jishuzhuanyong@example.cn"
        self.sender_pw = sender_pw or "Vip#123M"
        self.yag = yagmail.SMTP(
            user=self.sender, password=self.sender_pw, host='hwsmtp.exmail.qq.com')
        self.subject = subject

        if receiver:
            receiver.update(default_mail_recv)
            self.receiver = list(receiver)
        else:
            self.receiver = list(default_mail_recv)

    def __enter__(self):
        """进入上下文时，初始化SMTP连接并返回当前邮件实例

        返回:
            MyMail: 当前邮件实例
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文时，关闭邮件连接

        参数:
            exc_type: 异常类型
            exc_val: 异常值
            exc_tb: 异常追踪信息

        返回:
            bool: True表示正常退出，False表示发生异常
        """
        if self.yag:
            self.yag.close()  # 关闭连接
        # 如果发生异常，可以根据需要进行处理
        if exc_type:
            print('An error occurred: {}'.format(exc_val))
            return False  # 允许异常继续传播
        return True

    def add_data(self, new_data):
        """写入数据

        参数:
            new_data: 要添加的新数据
        """
        if not new_data.startswith('<h3>') or not new_data.startswith('<h3>') or not new_data.startswith('<h4>'):
            new_data = new_data + '\n'
        self.data.append(new_data)

    def set_mark(self, value=False):
        """设置标记

        参数:
            value: 标记值，用于控制邮件发送
        """
        self.mark = bool(value)
        self.data = "".join(self.data)

    def set_subject(self, subject):
        """设置邮件主题

        参数:
            subject: 邮件主题
        """
        self.subject = subject

    def send_email(self, subject=None, body=None):
        """发送邮件

        当标记为True且设置了主题和收件人时发送邮件
        发送完成后重置数据和标记

        异常:
            RuntimeError: 如果邮件客户端未初始化
        """
        if not self.yag:
            logging.error('请使用with语句初始化邮件客户端')
            return

        if not subject:
            subject = self.subject
        if not body:
            body = self.data

        if self.mark and subject and self.receiver:
            try:
                self.yag.send(to=self.receiver, subject=subject, contents=body)
                self.reset()
                logging.info('已发送邮件给 {}'.format(self.receiver))
            except Exception as e:
                logging.error('发送邮件失败 {},异常: {}'.format(self.receiver, e))

    def reset(self):
        """
        清空数据列表并将标记设置为False
        """
        self.subject = None
        self.data = []
        self.mark = False


class UpdateCert:
    def __init__(self, token=None, domain_info=None, env=None, local_dir=None, remote_dir=None, mail=None):
        self.mail = mail
        self.token_id = domain_info.get('token_id') or token.get('token_id')
        self.token_key = domain_info.get('token_key') or token.get('token_key')
        self.main_domain = domain_info.get('main_domain')
        self.sub_domains = domain_info.get('sub', [])
        self.hosts = domain_info.get('hosts', [])
        self.lock = threading.Lock()
        self.ssh_conf = {"key_filename": "ssh/id_rsa"}
        self.env = env
        self.key_name = '{}.key'.format(self.main_domain)
        self.crt_name = 'fullchain.cer'
        self.local_dir = local_dir or '/root/.acme.sh/{}_ecc/'.format(
            self.main_domain)
        self.remote_dir = remote_dir or '/usr/local/nginx/conf/ssl/{}/'.format(
            self.main_domain)
        self.crt_str = open(self.local_dir + self.crt_name, 'r').read()
        self.key_str = open(self.local_dir + self.key_name, 'r').read()
        self.reg = re.compile('^[a-zA-Z-_]+\.{}'.format(self.main_domain))

    def update(self):
        global is_update_ngx
        try:
            if not is_update_ngx:
                self.update_to_ngx()
                is_update_ngx = True

            self.update_to_tx()
            self.update_to_ali()
            self.update_to_qiniu()
        except Exception as e:
            logging.error(f"Falied: {e}", exc_info=True)

    def update_to_ngx(self, local_dir=None, remote_dir=None):
        local_dir = local_dir or self.local_dir
        remote_dir = remote_dir or self.remote_dir
        key_mapping = (local_dir + self.key_name, remote_dir + self.key_name)
        crt_mapping = (local_dir + self.crt_name,
                       remote_dir + self.main_domain + '.pem')

        logging.info('证书映射: {}'.format(crt_mapping))
        logging.info('私钥映射: {}'.format(key_mapping))

        self.mail.add_data('<h3>更新NGX</h3>')
        self.mail.add_data('<ul>')
        for host in self.hosts:
            try:
                host, port = host.split(':')
                with fabric.Connection(host=host, port=port, user='www' if self.env == 'prod' else 'root',
                                    connect_kwargs=self.ssh_conf or None) as ssh:
                    rest = ssh.run('mkdir -p {}'.format(remote_dir), hide=True)
                    if rest.return_code != 0:
                        logging.error('创建目录失败:{}, 错误:{}'.format(
                            remote_dir, rest.stderr))
                        continue
                    logging.debug(rest.stdout)
                    ssh.put(*crt_mapping)
                    ssh.put(*key_mapping)
                    rest = ssh.run('sudo /usr/local/nginx/sbin/nginx -t && sudo /usr/local/nginx/sbin/nginx -s reload',
                                hide=True)
                    if rest.return_code != 0:
                        logging.error('执行命令失败:{}'.format(rest.stderr))
                        continue
                    logging.debug(rest)
                    logging.info('证书同步到主机: {}'.format(host))
                    self.mail.add_data('<li>证书同步到主机: {}</li>'.format(host))
            except Exception as e:
                logging.error('更新NGX失败:{}'.format(e), exc_info=True)
                self.mail.add_data('<li>证书同步到主机失败: {}</li>'.format(host))
        self.mail.add_data('</ul>')

    def update_to_ali(self):
        pass

    def update_to_tx(self):
        pass

    def update_to_qiniu(self):
        pass

    def run_cmd(self, cmd, **kwargs):
        rest = subprocess.run(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, shell=True, **kwargs)
        rest.stderr = rest.stderr.decode().strip()
        rest.stdout = rest.stdout.decode().strip()
        if rest.stderr or rest.returncode != 0:
            logging.error('执行命令失败:{} \n错误信息:{}'.format(cmd, rest.stderr))
            return rest.stderr, False
        elif rest.stdout or rest.returncode == 0:
            logging.debug('执行命令成功:{} \n返回结果:{}'.format(
                cmd, rest.stdout.splitlines()))
            return rest.stdout, True

        logging.debug('无返回结果')
        return False, False

    def _get_update_cdn_ins(self, data, flag, ret_type='str'):
        update_cdn_ins = '' if ret_type == 'str' else []
        if flag:
            for cdn in data.splitlines():
                if ret_type == 'str':
                    update_cdn_ins = update_cdn_ins + '"{}" '.format(cdn)
                else:
                    update_cdn_ins.append(cdn)

        if not update_cdn_ins:
            logging.info('{} 无需要更新的CDN域名'.format(self.__class__.__name__))
            return False

        logging.info('{} 即将更新的CDN域名: {}'.format(
            self.__class__.__name__, update_cdn_ins))
        return update_cdn_ins


class TXCloud(UpdateCert):
    def update_to_tx(self):
        token = '--secretId {} --secretKey {}'.format(
            self.token_id, self.token_key)
        cmd = '''tccli cdn DescribeDomains {} \
            --cli-unfold-argument | \
            jq -r '.Domains[] | select(.Status == "online") | select(.Domain | contains("{}")) | .Domain'
        '''.format(token, self.main_domain)
        rest, flag = self.run_cmd(cmd)
        update_cdn_ins = self._get_update_cdn_ins(rest, flag)

        if update_cdn_ins:
            self.mail.add_data('<h3>更新腾讯云CDN</h3>')
            self.mail.add_data('<ul>')
            cmd = '''tccli ssl UploadCertificate {} \
                --cli-unfold-argument \
                --CertificatePublicKey "{}" \
                --CertificatePrivateKey "{}" \
                --Alias {} | jq -r .CertificateId
            '''.format(token, self.crt_str, self.key_str, self.main_domain)
            cert_id, flag = self.run_cmd(cmd)
            if flag:
                logging.info('证书ID: {}'.format(cert_id))
                cmd = '''
                    tccli ssl DeployCertificateInstance {} \
                    --cli-unfold-argument \
                    --CertificateId "{}" \
                    --InstanceIdList {} \
                    --ResourceType cdn |jq -r .DeployRecordId
                '''.format(token, cert_id, update_cdn_ins)
                deploy_id, flag = self.run_cmd(cmd)
                if flag:
                    logging.info('部署任务ID: {}'.format(deploy_id))
                    cmd = '''tccli ssl DescribeHostDeployRecordDetail {} \
                        --cli-unfold-argument --DeployRecordId {} | jq -r 'select(.TotalCount == .SuccessTotalCount) | .DeployRecordDetailList[].Domains[]'
                    '''.format(token, deploy_id)
                    rest, flag = self.run_cmd(cmd)
                    if flag:
                        _ = '更新cdn域名证书成功'
                        logging.info('{}: {}'.format(_, update_cdn_ins))
                        self.mail.add_data(
                            '<li>{}: {}</li>'.format(_, update_cdn_ins))
                    else:
                        _ = '更新cdn域名证书失败'
                        logging.error('{}: {}'.format(_, rest))
                        self.mail.add_data(
                            '<li>{}: {}</li>'.format(_, update_cdn_ins))

            self.mail.add_data('</ul>')


class AliCloud(UpdateCert):
    def update_to_ali(self):
        token = '--access-key-id {} --access-key-secret {}'.format(
            self.token_id, self.token_key)
        cmd = '''aliyun cdn DescribeCdnHttpsDomainList --region cn-guangzhou {} | \
            jq -r '.CertInfos.CertInfo[] | select(.DomainName | contains("{}")) | .DomainName'
        '''.format(token, self.main_domain)
        rest, flag = self.run_cmd(cmd)
        update_cdn_ins = self._get_update_cdn_ins(rest, flag, ret_type='list')

        if update_cdn_ins:
            self.mail.add_data('<h3>更新阿里云cdn</h3>')
            self.mail.add_data('<ul>')
            for domain in update_cdn_ins:
                cmd = '''aliyun cdn SetCdnDomainSSLCertificate {} \
                     --DomainName {} --SSLPub="{}" --SSLPri="{}" --region cn-guangzhou --CertType upload --SSLProtocol on
                '''.format(token, domain, self.crt_str, self.key_str)
                rest, flag = self.run_cmd(cmd)
                if flag:
                    _ = '阿里云更新cdn域名证书成功'
                    logging.info('{}: {}'.format(_, domain))
                else:
                    _ = '阿里云更新cdn域名证书失败'
                    logging.error('{}: {}'.format(_, rest))
                self.mail.add_data('<li>{}: {}</li>'.format(_, domain))
            self.mail.add_data('</ul>')


class QiniuCloud(UpdateCert):
    def update_to_qiniu(self):
        update_cdn = []
        auth = Auth(self.token_id, self.token_key)
        domain_mgr = DomainMgr(auth)
        ret, info = domain_mgr.list_domains()
        if ret.get('error'):
            logging.error('七牛云获取域名列表失败: {}'.format(ret.get('error')))
        else:
            for _ in ret.get('domains'):
                # logging.info(_)
                if self.reg.match(_.get('name')):
                    # logging.info(_.get('name'))
                    is_ssl = False if _.get('protocol') == 'http' else True
                    update_cdn.append((_.get('name'), is_ssl))

        if not update_cdn:
            logging.info('{} 无需要更新的CDN域名'.format(self.__class__.__name__))
            return

        logging.info('{} 即将更新的CDN域名: {}'.format(
            self.__class__.__name__, update_cdn))

        self.mail.add_data('<h3>更新七牛云CDN</h3>')
        self.mail.add_data('<ul>')
        ret, info = domain_mgr.create_sslcert(
            name=self.main_domain,
            common_name=self.main_domain,
            pri=self.key_str,
            ca=self.crt_str
        )
        if ret.get('error'):
            _ = '七牛云创建证书失败'
            logging.error('{}: {}'.format(_, ret.get('error')))
        else:
            _ = '七牛云更新cdn域名证书成功'
            logging.info(ret['certID'])
            for domain, is_ssl_flag in update_cdn:
                new_ret, info = domain_mgr.put_httpsconf(
                    domain, ret['certID'], is_ssl_flag)
                self.mail.add_data('<li>{}: {}</li>'.format(_, domain))

            logging.info('{}: {}'.format(_, update_cdn))

        self.mail.add_data('</ul>')


def main(name=None, env=None):
    mail = MyMail(subject='证书更新通知')
    mail.add_data('<h1>证书更新通知报表</h1>')
    default_flag = False

    default_tx_token = {
        'token_id': '',
        'token_key': ''
    }
    default_ali_token = {
        'token_id': '',
        'token_key': ''
    }
    default_qiniu_token = {
        'token_id': '',
        'token_key': ''
    }

    default_port = 2233

    if env == 'prod':
        pwd = None
        lb_hosts = {
            'gz': ('1.1.1.1:{}'.format(default_port),),
            'hk': ('1.1.1.1:{}'.format(default_port),),
            'us': ('1.1.1.1:22',),
            'mg': ('1.1.1.1:{}'.format(default_port),),
            'qs': ('1.1.1.1:{}'.format(default_port), '1.1.1.1:{}'.format(default_port)),
            'ms': ('1.1.1.1:{}'.format(default_port),)
        }
        crt_info = {
            'tx': (
                default_tx_token,
                {
                    'mk': {
                        'main_domain': 'example.com',
                        'hosts': {
                            *lb_hosts['gz'], *lb_hosts['hk'], *lb_hosts['us']
                        }
                    },
                    'erp': {
                        'main_domain': 'example.cn',
                        'hosts': {
                            *lb_hosts['gz'], *lb_hosts['hk'], *lb_hosts['us']
                        }
                    },
                    'qs': {
                        'token_id': '',
                        'token_key': '',
                        'main_domain': 'example.com',
                        'hosts': {
                            *lb_hosts['qs']
                        }
                    },
                    'wms': {
                        'main_domain': 'wms.example.com',
                        'hosts': {
                            *lb_hosts['gz'], *lb_hosts['ms']
                        }
                    },
                    'tp': {
                        'main_domain': 'example.cn',
                        'hosts': {
                            *lb_hosts['gz']
                        }
                    },
                    'tpcb': {
                        'main_domain': 'cb.example.cn',
                        'hosts': {
                            *lb_hosts['gz']
                        }
                    },
                    'ms': {
                        'token_id': '',
                        'token_key': '',
                        'main_domain': 'example.com',
                        'hosts': {
                            *lb_hosts['gz'], *lb_hosts['ms']
                        }
                    }
                }
            ),
            'ali': (
                default_ali_token,
                {
                    'mg': {
                        'main_domain': 'example.com',
                        'hosts': {*lb_hosts['mg']}
                    },
                }
            ),
            'qiniu': (
                default_qiniu_token,
                {
                    'qs': {
                        'token_id': '',
                        'token_key': '',
                        'main_domain': 'example.com'
                    },
                    'mk': {
                        'main_domain': 'example.com'
                    },
                    'erp': {
                        'main_domain': 'example.cn'
                    },
                    'tp': {
                        'main_domain': 'example.cn'
                    },
                    'ms': {
                        'main_domain': 'example.com'
                    }
                }
            )
        }
    else:
        pwd = '{}/'.format(os.getcwd())
        lb_hosts = {
            'gz': ('1.1.1.1:22',)
        }
        crt_info = {
            'tx': (
                default_tx_token,
                {
                    'mkt2': {
                        'main_domain': 'test2.example.com',
                        'hosts': {
                            *lb_hosts['gz']
                        }
                    }
                }
            )
        }

    for cloud in crt_info.keys():
        token, domain = crt_info[cloud]
        if not name in domain.keys():
            logging.warning('未找到配置: {} - {}'.format(cloud, name))
            continue

        if not default_flag:
            mail.add_data('本次更新证书: {}'.format(domain[name].get('main_domain')))
            default_flag = True

        if cloud == 'tx':
            tx = TXCloud(token, domain[name], env=env, local_dir=pwd, mail=mail)
            tx.update()

        elif cloud == 'ali':
            ali = AliCloud(token, domain[name], env=env,
                           local_dir=pwd, mail=mail)
            ali.update()

        elif cloud == 'qiniu':
            qiniu = QiniuCloud(token, domain[name],
                               env=env, local_dir=pwd, mail=mail)
            qiniu.update()
            # qiniu.update_to_qiniu()

        logging.info('更新配置完成: {}'.format(name))

    mail.set_mark(True)
    mail.send_email()
    mail.yag.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    default_mail_recv = {
        "qq@qq.com"
    }
    is_update_ngx = False

    parser = argparse.ArgumentParser(description="Update SSL certificate for domains.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-m', '--message', help='通知消息，若提供则直接发送邮件并退出')
    group.add_argument('-e', '--env', choices=['prod', 'test'], help='Environment: prod or test')
    parser.add_argument('-n', '--name', help='Configuration name to use in crt_info')

    args = parser.parse_args()

    if args.message:
        mail = MyMail(subject='证书更新通知')
        mail.add_data(args.message)
        mail.set_mark(True)
        mail.send_email()
        mail.yag.close()
        exit(0)
    elif args.env:
        if not args.name:
            parser.error('参数 -e 存在时，-n 也必须提供')
        
        try:
            main(args.name, args.env)
        except:
            logging.error(exc_info=True)
    else:
        parser.error('必须指定 -m 或 -e')

##### 运行
#export Ali_Key=""
#export Ali_Secret=""
#acme.sh --issue \
# --dns dns_ali \
# -d example.com \
# -d '*.example.com' \
# --server letsencrypt \
#  --renew-hook "cd /opt/scripts/ ;python3 cert-update.py -e prod -n mg"
