#!/usr/bin/env python3
"""
Simulador de eventos de e-commerce para Kafka/Redpanda.

Gera eventos realistas de funil: page_view -> view_item -> add_to_cart -> purchase
com sessões de usuários, catálogo de produtos e comportamento temporal.
"""
import asyncio
import json
import random
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from faker import Faker

from config import config
from events import (
    EcommerceEvent,
    EventType,
    UserSession,
    DeviceType,
)


fake = Faker("pt_BR")


class ProductCatalog:
    """Catálogo estático de produtos para JOIN com dim_products no dbt"""

    def __init__(self, size: int, category_count: int):
        self.products = self._generate_catalog(size, category_count)
        self.categories = self._generate_categories(category_count)

    def _generate_categories(self, count: int) -> List[Dict]:
        categories = [
            {"id": "cat_eletronicos", "name": "Eletrônicos"},
            {"id": "cat_roupas", "name": "Roupas e Acessórios"},
            {"id": "cat_casa", "name": "Casa e Decoração"},
            {"id": "cat_id": "cat_beleza", "name": "Beleza e Cuidados"},
            {"id": "cat_esportes", "name": "Esportes e Lazer"},
            {"id": "cat_livros", "name": "Livros"},
            {"id": "cat_brinquedos", "name": "Brinquedos"},
            {"id": "cat_automotivo", "name": "Automotivo"},
            {"id": "cat_alimentos", "name": "Alimentos e Bebidas"},
            {"id": "cat_pet", "name": "Pet Shop"},
        ]
        return categories[:count]

    def _generate_catalog(self, size: int, category_count: int) -> List[Dict]:
        products = []
        brands = ["TechPro", "StyleMax", "HomeComfort", "BeautyGlow", "SportForce",
                  "BookWorld", "ToyJoy", "AutoParts", "FoodieDelight", "PetCare"]

        for i in range(size):
            cat = self.categories[i % category_count]
            products.append({
                "product_id": f"prod_{i:04d}",
                "category_id": cat["id"],
                "category_name": cat["name"],
                "brand": random.choice(brands),
                "price": round(random.uniform(19.90, 2999.90), 2),
                "currency": "BRL",
                "is_active": True,
            })
        return products

    def get_random_product(self) -> Dict:
        return random.choice(self.products)

    def get_product(self, product_id: str) -> Optional[Dict]:
        for p in self.products:
            if p["product_id"] == product_id:
                return p
        return None


class UserSessionManager:
    """Gerencia pool de usuários e sessões ativas"""

    def __init__(self, pool_size: int, session_duration_min: int):
        self.pool_size = pool_size
        self.session_duration = timedelta(minutes=session_duration_min)
        self.sessions: Dict[str, UserSession] = {}
        self._init_user_pool()

    def _init_user_pool(self):
        """Cria pool inicial de usuários anônimos"""
        for i in range(self.pool_size):
            user_id = f"user_{i:06d}"
            # Pré-cria sessão para cada usuário
            self._create_session(user_id)

    def _create_session(self, user_id: str) -> UserSession:
        session_id = f"sess_{fake.uuid4()[:8]}"
        device = random.choice(list(DeviceType))
        session = UserSession(
            user_id=user_id,
            session_id=session_id,
            started_at=datetime.utcnow(),
            last_event_at=datetime.utcnow(),
            device_type=device,
            user_agent=fake.user_agent(),
            ip_country="BR",
        )
        self.sessions[session_id] = session
        return session

    def get_or_create_session(self, user_id: str) -> UserSession:
        """Pega sessão ativa ou cria nova se expirada"""
        # Procura sessão ativa do usuário
        for sess in self.sessions.values():
            if sess.user_id == user_id:
                if datetime.utcnow() - sess.last_event_at < self.session_duration:
                    return sess
                # Sessão expirada
                break

        # Cria nova sessão
        return self._create_session(user_id)

    def get_random_user(self) -> str:
        return f"user_{random.randint(0, self.pool_size - 1):06d}"


class EventGenerator:
    """Gera eventos seguindo funil probabilístico realista"""

    FUNNEL_STEPS = [
        (EventType.PAGE_VIEW, 0),
        (EventType.VIEW_ITEM, 1),
        (EventType.ADD_TO_CART, 2),
        (EventType.PURCHASE, 3),
    ]

    def __init__(self, catalog: ProductCatalog, session_mgr: UserSessionManager):
        self.catalog = catalog
        self.session_mgr = session_mgr
        self.probabilities = {
            EventType.PAGE_VIEW: config.prob_page_view,
            EventType.VIEW_ITEM: config.prob_view_item,
            EventType.ADD_TO_CART: config.prob_add_to_cart,
            EventType.PURCHASE: config.prob_purchase,
        }

    def generate_event(self) -> EcommerceEvent:
        """Gera próximo evento baseado no estado da sessão"""
        user_id = self.session_mgr.get_random_user()
        session = self.session_mgr.get_or_create_session(user_id)

        # Decide próximo evento baseado no funil
        event_type = self._decide_next_event(session)

        # Enriquece com dados do produto se aplicável
        product_data = {}
        if event_type in [EventType.VIEW_ITEM, EventType.ADD_TO_CART, EventType.PURCHASE]:
            product = self.catalog.get_random_product()
            product_data = {
                "product_id": product["product_id"],
                "category_id": product["category_id"],
                "category_name": product["category_name"],
                "brand": product["brand"],
                "price": product["price"],
            }

            if event_type == EventType.VIEW_ITEM:
                session.viewed_products.append(product["product_id"])
            elif event_type == EventType.ADD_TO_CART:
                session.cart.append(product["product_id"])
            elif event_type == EventType.PURCHASE:
                # Compra itens do carrinho ou produto visualizado
                pass

        event = EcommerceEvent(
            user_id=session.user_id,
            session_id=session.session_id,
            event_type=event_type,
            event_timestamp=datetime.utcnow(),
            page_url=fake.url() if event_type == EventType.PAGE_VIEW else None,
            referrer_url=fake.url() if random.random() < 0.3 else None,
            user_agent=session.user_agent,
            ip_country=session.ip_country,
            device_type=session.device_type,
            os=fake.linux_platform_token() if session.device_type == DeviceType.DESKTOP else fake.android_platform_token(),
            browser=fake.chrome_version() if random.random() < 0.7 else fake.safari_version(),
            **product_data,
        )

        # Atualiza estado da sessão
        session.last_event_at = datetime.utcnow()
        session.current_step = max(session.current_step, self.FUNNEL_STEPS[[e for e, s in self.FUNNEL_STEPS].index(event_type)][1])

        return event

    def _decide_next_event(self, session: UserSession) -> EventType:
        """Escolhe próximo evento baseado no funil e probabilidades"""
        # Se nunca fez page_view, força page_view
        if session.current_step == 0:
            return EventType.PAGE_VIEW

        # Probabilidades condicionais
        if session.current_step == 1:  # Fez page_view
            choices = [EventType.PAGE_VIEW, EventType.VIEW_ITEM]
            weights = [0.3, 0.7]
        elif session.current_step == 2:  # Fez view_item
            choices = [EventType.VIEW_ITEM, EventType.ADD_TO_CART, EventType.PAGE_VIEW]
            weights = [0.4, 0.4, 0.2]
        elif session.current_step == 3:  # Fez add_to_cart
            choices = [EventType.ADD_TO_CART, EventType.PURCHASE, EventType.VIEW_ITEM]
            weights = [0.2, 0.5, 0.3]
        else:  # Fez purchase - nova sessão ou continua navegando
            choices = [EventType.PAGE_VIEW, EventType.VIEW_ITEM]
            weights = [0.6, 0.4]

        return random.choices(choices, weights=weights)[0]


class KafkaProducer:
    """Wrapper simples para kafka-python com retry e métricas"""

    def __init__(self, bootstrap_servers: str, topic: str):
        from kafka import KafkaProducer
        from kafka.errors import KafkaError

        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
            enable_idempotence=True,
            compression_type="snappy",
            linger_ms=10,
            batch_size=16384,
        )
        self.sent_count = 0
        self.error_count = 0

    def send(self, event: EcommerceEvent) -> bool:
        try:
            future = self.producer.send(
                self.topic,
                key=event.user_id,
                value=event.to_kafka_message(),
            )
            # Block até confirmar (para simplicidade do simulador)
            future.get(timeout=10)
            self.sent_count += 1
            return True
        except Exception as e:
            self.error_count += 1
            print(f"[ERROR] Falha ao enviar evento: {e}", file=sys.stderr)
            return False

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()

    def stats(self) -> dict:
        return {
            "sent": self.sent_count,
            "errors": self.error_count,
            "error_rate": self.error_count / max(self.sent_count, 1),
        }


async def run_simulator():
    """Loop principal do simulador"""
    print("🚀 Iniciando simulador de e-commerce...")
    print(f"   Broker: {config.kafka_bootstrap_servers}")
    print(f"   Topic: {config.kafka_topic}")
    print(f"   Taxa: {config.events_per_second} eventos/seg")
    print(f"   Usuários: {config.user_pool_size}")
    print(f"   Produtos: {config.product_catalog_size}")
    print("   Pressione Ctrl+C para parar\n")

    # Inicializa componentes
    catalog = ProductCatalog(config.product_catalog_size, config.category_count)
    session_mgr = UserSessionManager(config.user_pool_size, config.session_duration_minutes)
    generator = EventGenerator(catalog, session_mgr)
    producer = KafkaProducer(config.kafka_bootstrap_servers, config.kafka_topic)

    # Controle de taxa
    interval = 1.0 / config.events_per_second
    last_stats = time.time()

    # Signal handling
    shutdown = asyncio.Event()

    def signal_handler(signum, frame):
        print("\n🛑 Sinal de parada recebido...")
        shutdown.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while not shutdown.is_set():
            start = time.time()

            # Gera e envia evento
            event = generator.generate_event()
            producer.send(event)

            # Stats a cada 10 segundos
            if time.time() - last_stats > 10:
                stats = producer.stats()
                print(f"📊 Enviados: {stats['sent']} | Erros: {stats['errors']} | Taxa erro: {stats['error_rate']:.2%}")
                last_stats = time.time()

            # Rate limiting
            elapsed = time.time() - start
            sleep_time = max(0, interval - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

    finally:
        producer.flush()
        producer.close()
        final = producer.stats()
        print(f"\n✅ Simulador finalizado. Total: {final['sent']} eventos, {final['errors']} erros")


if __name__ == "__main__":
    asyncio.run(run_simulator())
