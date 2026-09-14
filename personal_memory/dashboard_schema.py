"""Copied as the plugin root config_schema.py; pure host configuration declaration."""
from plugins.memory.config_schema import ProviderConfigSchema,ProviderField,KIND_NUMBER,KIND_JSON

CONFIG_SCHEMA=ProviderConfigSchema(name='personal-memory',label='Personal Memory',fields=(
    ProviderField(key='port',label='Service port',kind=KIND_NUMBER,default='8766',inline=True,
                  description='Local port 1024..65535. Restart service and Hermes after changes.'),
    ProviderField(key='prefetch_wait_ms',label='Recall wait (ms)',kind=KIND_NUMBER,default='200',inline=True,
                  description='Bounded result wait, 0..2000 milliseconds.'),
    ProviderField(key='session_access',label='Owner identities',kind=KIND_JSON,default='{"owners":{}}',
                  description='Platform-to-stable-owner-ID lists under owners. Shared chats are always denied.'),
    ProviderField(key='retrieval',label='Retrieval engines',kind=KIND_JSON,default='{"hindsight":{"managed":true,"sources":["*"]}}',
                  description='Hindsight is always enabled. By default it runs privately on loopback; an external URL explicitly authorizes disclosure to that endpoint.'),
))
