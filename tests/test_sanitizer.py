"""
Testes Unitários da Camada Zero-Exposure (LGPD) — `src/sanitizer.py`.
Garante determinismo do hash SHA-256 e bloqueio total de PII (e-mail, CPF, IP e nome).
"""

import json
from pathlib import Path
import re
import sys

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.sanitizer import (
    generate_user_hash,
    mask_ip_address,
    sanitize_event_payload,
    sanitize_free_text,
)


def test_generate_user_hash_is_deterministic_and_case_insensitive():
    """O mesmo e-mail com espaços ou letras maiúsculas deve produzir exatamente o mesmo hash SHA-256."""
    hash_1 = generate_user_hash("Player.One@ValkyrieGames.com", salt="test-salt")
    hash_2 = generate_user_hash("  player.one@valkyriegames.com  ", salt="test-salt")

    assert hash_1 == hash_2
    assert len(hash_1) == 64
    assert re.fullmatch(r"[0-9a-f]{64}", hash_1) is not None


def test_generate_user_hash_normalizes_formatted_cpf():
    """CPFs com ou sem pontuação devem resultar no mesmo hash determinístico."""
    hash_formatted = generate_user_hash("123.456.789-01", salt="test-salt")
    hash_digits = generate_user_hash("12345678901", salt="test-salt")

    assert hash_formatted == hash_digits


def test_mask_ip_address_preserves_only_subnet():
    """Endereços IPv4 e IPv6 devem ter os octetos finais mascarados."""
    assert mask_ip_address("189.45.210.99") == "189.45.xxx.xxx"
    assert mask_ip_address("2001:0db8:85a3:0000:0000:8a2e:0370:7334") == "2001:0db8:xxxx:xxxx"
    assert mask_ip_address(None) == "0.0.xxx.xxx"


def test_sanitize_free_text_scrubs_email_cpf_ip_and_name():
    """Texto livre contendo e-mail, CPF, IP e nome do jogador não pode vazar nenhum dado."""
    dirty_log = (
        "Erro de sessão para user_name=Lucas Oliveira, email=lucas.oliveira@gmail.com, "
        "CPF 456.789.123-99 conectado via IP 177.88.99.100."
    )
    cleaned = sanitize_free_text(dirty_log, known_name="Lucas Oliveira")

    assert "lucas.oliveira@gmail.com" not in cleaned
    assert "456.789.123-99" not in cleaned
    assert "177.88.99.100" not in cleaned
    assert "Lucas Oliveira" not in cleaned
    assert "[REDACTED_EMAIL]" in cleaned
    assert "[REDACTED_CPF]" in cleaned
    assert "177.88.xxx.xxx" in cleaned


def test_sanitize_event_payload_enforces_zero_exposure():
    """O dicionário sanitizado nunca deve conter chaves ou valores brutos de PII."""
    raw_event = {
        "event_id": "evt_zero_exp_001",
        "tenant_id": "tenant_01",
        "tenant_name": "Studio Alpha",
        "product_id": "prod_game_01",
        "product_name": "Cyber Quest",
        "category": "GAME",
        "user_identifier": "mariana.costa@empresa.com.br",
        "user_name": "Mariana Costa",
        "user_cpf": "987.654.321-11",
        "ip_address": "200.150.10.25",
        "tier": "PRO",
        "event_type": "PURCHASE",
        "session_duration_sec": 600,
        "latency_ms": 42.5,
        "revenue_brl": 99.90,
        "raw_metadata": "Compra aprovada para Mariana Costa (CPF 987.654.321-11) IP 200.150.10.25",
        "occurred_at": "2026-11-10T15:30:00Z",
    }

    sanitized = sanitize_event_payload(raw_event, salt="unit-test-salt")
    serialized = json.dumps(sanitized)

    # Verifica que campos brutos de PII foram eliminados do contrato persistível
    assert "user_identifier" not in sanitized
    assert "user_name" not in sanitized
    assert "user_cpf" not in sanitized
    assert "ip_address" not in sanitized

    # Verifica que nenhum valor sensível sobreviveu em qualquer campo do JSON final
    assert "mariana.costa@empresa.com.br" not in serialized
    assert "Mariana Costa" not in serialized
    assert "987.654.321-11" not in serialized
    assert "200.150.10.25" not in serialized
    assert len(sanitized["user_hash"]) == 64
