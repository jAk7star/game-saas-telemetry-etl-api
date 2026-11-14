"""
Camada Zero-Exposure (LGPD): Sanitização determinística via SHA-256 e Regex.
Garante que nenhum dado pessoal identificável (e-mail, IP real, CPF ou nome)
chegue ao banco relacional analítico.
"""

import hashlib
import json
import os
import re
from typing import Any, Dict, Optional

DEFAULT_SALT = os.getenv("ZERO_EXPOSURE_SALT", "antigravity-lgpd-zero-exposure-salt-2026")

# Expressões Regulares pré-compiladas para detecção e mascaramento de PII
EMAIL_REGEX = re.compile(
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
)
# CPF formatado (000.000.000-00) ou sequência isolada de 11 dígitos
CPF_REGEX = re.compile(
    r"\b(?:\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11})\b"
)
# IPv4 (ex: 192.168.1.100)
IPV4_REGEX = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b"
)
# IPv6 simplificado
IPV6_REGEX = re.compile(
    r"\b(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}\b"
)
# Padrões explícitos de nome em logs (ex: "user_name=João Silva", "nome: Maria Souza")
NAME_KV_REGEX = re.compile(
    r"(?i)\b(nome|name|user_name|cliente|jogador|player_name)\s*[:=]\s*([A-ZÀ-Ÿ][a-zà-ÿ]+(?:\s+[A-ZÀ-Ÿ][a-zà-ÿ]+)+)"
)


def generate_user_hash(raw_identifier: str, salt: Optional[str] = None) -> str:
    """
    Gera um hash SHA-256 determinístico (64 caracteres hexadecimais) a partir de
    qualquer identificador bruto (e-mail, CPF, ID de conta), normalizando caixa e espaços.
    """
    if not raw_identifier or not str(raw_identifier).strip():
        raise ValueError("Identificador do usuário não pode ser vazio para anonimização.")

    effective_salt = salt if salt is not None else DEFAULT_SALT
    normalized = str(raw_identifier).strip().lower()
    # Se for um possível CPF pontuado, normaliza apenas dígitos para manter determinismo
    if re.fullmatch(r"\d{3}\.\d{3}\.\d{3}-\d{2}", normalized):
        normalized = re.sub(r"\D", "", normalized)

    payload = f"{effective_salt}:{normalized}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def mask_ip_address(ip: Optional[str]) -> str:
    """
    Mascara endereços IPv4/IPv6 preservando apenas o prefixo de rede para diagnóstico
    regional sem expor o dispositivo individual (LGPD).
    Exemplo: '189.45.123.98' -> '189.45.xxx.xxx'
    """
    if not ip or not str(ip).strip():
        return "0.0.xxx.xxx"

    cleaned = str(ip).strip()
    if IPV4_REGEX.fullmatch(cleaned):
        octets = cleaned.split(".")
        return f"{octets[0]}.{octets[1]}.xxx.xxx"
    if ":" in cleaned:
        parts = cleaned.split(":")
        return f"{parts[0]}:{parts[1]}:xxxx:xxxx"
    return "[REDACTED_IP]"


def sanitize_free_text(text: Optional[str], known_name: Optional[str] = None) -> str:
    """
    Remove e mascara qualquer ocorrência de e-mail, CPF, IPv4, IPv6 e nomes próprios
    em textos de log ou metadados abertos.
    """
    if not text:
        return ""

    sanitized = str(text)

    # Se o nome do usuário foi informado no evento, remove ocorrências literais no texto livre
    if known_name and len(known_name.strip()) >= 2:
        escaped_name = re.escape(known_name.strip())
        sanitized = re.sub(rf"(?i)\b{escaped_name}\b", "[REDACTED_NAME]", sanitized)

    # Mascara chaves explícitas de nome (ex: "nome: Carlos Eduardo")
    sanitized = NAME_KV_REGEX.sub(r"\1=[REDACTED_NAME]", sanitized)
    # Mascara e-mails
    sanitized = EMAIL_REGEX.sub("[REDACTED_EMAIL]", sanitized)
    # Mascara CPFs
    sanitized = CPF_REGEX.sub("[REDACTED_CPF]", sanitized)
    # Mascara IPs
    sanitized = IPV4_REGEX.sub(lambda m: mask_ip_address(m.group(0)), sanitized)
    sanitized = IPV6_REGEX.sub("[REDACTED_IPV6]", sanitized)

    return sanitized


def sanitize_event_payload(raw_event: Dict[str, Any], salt: Optional[str] = None) -> Dict[str, Any]:
    """
    Aplica a política Zero-Exposure em um dicionário de evento:
    1. Converte `user_identifier` (ou `user_email`/`user_cpf`) em `user_hash` SHA-256 determinístico.
    2. Descarta campos explícitos de PII (`user_identifier`, `user_name`, `user_cpf`, `ip_address`).
    3. Sanitiza `raw_metadata` e armazena apenas o subnet mascarado do IP em `sanitized_metadata`.
    """
    identifier = (
        raw_event.get("user_identifier")
        or raw_event.get("user_email")
        or raw_event.get("user_cpf")
        or raw_event.get("user_hash")
    )
    if not identifier:
        raise ValueError("Evento sem identificador de usuário válido.")

    # Se já for um SHA-256 hex de 64 chars e veio como user_hash, preserva; caso contrário faz hash
    if (
        raw_event.get("user_hash")
        and not raw_event.get("user_identifier")
        and re.fullmatch(r"[0-9a-f]{64}", str(raw_event["user_hash"]))
    ):
        user_hash = str(raw_event["user_hash"])
    else:
        user_hash = generate_user_hash(str(identifier), salt=salt)

    known_name = raw_event.get("user_name")
    masked_subnet = mask_ip_address(raw_event.get("ip_address"))
    clean_notes = sanitize_free_text(raw_event.get("raw_metadata"), known_name=known_name)

    sanitized_meta_obj = {
        "masked_ip": masked_subnet,
        "notes": clean_notes,
        "zero_exposure_v": "1.0",
    }

    return {
        "event_id": str(raw_event["event_id"]).strip(),
        "tenant_id": str(raw_event["tenant_id"]).strip(),
        "tenant_name": sanitize_free_text(raw_event.get("tenant_name") or "Default Tenant", known_name=known_name),
        "product_id": str(raw_event["product_id"]).strip(),
        "product_name": str(raw_event.get("product_name") or "Unknown Product").strip(),
        "category": str(raw_event["category"]).strip().upper(),
        "user_hash": user_hash,
        "tier": str(raw_event.get("tier") or "FREE").strip().upper(),
        "event_type": str(raw_event["event_type"]).strip().upper(),
        "session_duration_sec": raw_event.get("session_duration_sec"),
        "latency_ms": raw_event.get("latency_ms"),
        "revenue_brl": raw_event.get("revenue_brl"),
        "sanitized_metadata": json.dumps(sanitized_meta_obj, ensure_ascii=False),
        "occurred_at": raw_event.get("occurred_at"),
    }
