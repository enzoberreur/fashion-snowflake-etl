from __future__ import annotations

import argparse
import json
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from faker import Faker


@dataclass
class GenerationConfig:
    sales: int = 0
    products: int = 0
    customers: int = 0
    suppliers: int = 0
    stores: int = 0
    promotions: int = 10
    returns: int = 20
    reviews: int = 50
    inventory: int = 50
    output_dir: Path = Path("data")

    def __post_init__(self) -> None:
        # Auto-calcul des ratios
        if self.suppliers == 0:
            self.suppliers = max(3, self.products // 20)

        self.output_dir.mkdir(exist_ok=True)

class DataGenerator:

    def __init__(self, config: GenerationConfig) -> None:
        self.config = config
        self.fake = Faker()

        # 🔑 ID RANGES
        self.ranges = {
            'supplier': (1, max(1, config.suppliers)),
            'customer': (1001, max(1001, 1001 + config.customers - 1)),
            'product': (2001, max(2001, 2001 + config.products - 1)),
            'store': (3001, max(3001, 3001 + config.stores - 1)),
            'sale': (100001, max(100001, 100001 + config.sales - 1)),
            'promotion': (4001, max(4001, 4001 + config.promotions - 1)),
            'return': (5001, max(5001, 5001 + config.returns - 1)),
            'review': (6001, max(6001, 6001 + config.reviews - 1)),
            'inventory': (7001, max(7001, 7001 + config.inventory - 1))
        }

        print("🎯 ID Ranges configurés pour cohérence parfaite:")
        for entity, (start, end) in self.ranges.items():
            print(f"   {entity.title()}: {start}-{end} ({end-start+1} items)")

    def id_range_iterator(self, entity: str) -> Iterator[int]:
        """Générateur d'IDs séquentiels pour un type d'entité"""
        start, end = self.ranges[entity]
        yield from range(start, end + 1)

    def random_id_from_range(self, entity: str) -> int:
        """ID aléatoire dans une range (pour références croisées)"""
        start, end = self.ranges[entity]
        return self.fake.random_int(min=start, max=end)

    # =================== GÉNÉRATEURS COMPATIBLES ===================

    def generate_suppliers(self) -> Iterator[dict[str, Any]]:
        vintage_suppliers = [
            "Maisons de Couture Parisiennes", "Ateliers Vintage Milano", "London Vintage Collective",
            "New York Estate Sales", "Tokyo Vintage Market", "Collectors Européens",
            "Vestiaire de Luxe", "Archives Mode & Style", "Vintage Couture House"
        ]
        vintage_specialties = ["Haute Couture Vintage", "Prêt-à-Porter de Luxe", "Accessoires Vintage", "Pièces Rares Collection"]
        vintage_locations = ["Paris", "Milan", "London", "New York", "Tokyo", "Anvers", "Florence"]

        for supplier_id in self.id_range_iterator('supplier'):
            supplier_name = self.fake.random_element(vintage_suppliers)

            yield {
                'supplier_id': f"SUP{supplier_id:03d}",
                'name': supplier_name,
                'contact_person': self.fake.name(),
                'email': f"contact@{supplier_name.lower().replace(' ', '').replace('.', '')}.com",
                'phone': self.fake.phone_number(),
                'address': json.dumps({
                    'street': self.fake.street_address(),
                    'city': self.fake.random_element(vintage_locations),
                    'country': self.fake.random_element(["France", "Italy", "United Kingdom", "United States", "Japan", "Belgium"])
                }),
                'specialty': self.fake.random_element(vintage_specialties),
                'lead_time_days': self.fake.random_int(7, 45),
                'minimum_order': self.fake.random_int(200, 2000),
                'payment_terms': self.fake.random_element(["Net 30", "Net 45", "Virement immédiat"]),
                'quality_rating': round(self.fake.random.uniform(4.2, 5.0), 1),
                'established_date': self.fake.date_between('-25y', '-3y').isoformat(),
                'is_active': self.fake.boolean(95),

                'vintage_era_focus': self.fake.random_element(['1950s-60s', '1970s-80s', '1990s-2000s', 'Multi-époque']),
                'authentication_service': self.fake.boolean(85),
                'certifications': self.fake.random_element(['Authentifié Vestiaire', 'Certifié Maison', 'Expertise Indépendante'])
            }

    def generate_products(self) -> Iterator[dict[str, Any]]:
        """Génère products fashion vintage - Compatible TOUS ingesters"""
        vintage_categories = ['Robes', 'Vestes', 'Pantalons', 'Jupes', 'Chemises', 'Manteaux', 'Blouses', 'Accessoires']
        vintage_brands = ['Chanel', 'Dior', 'Yves Saint Laurent', 'Hermès', 'Prada', 'Gucci', 'Versace', 'Valentino', 'Céline', 'Givenchy']
        vintage_materials = ['Soie', 'Laine', 'Cuir véritable', 'Coton', 'Cachemire', 'Velours', 'Tweed', 'Dentelle']
        vintage_colors = ['Noir', 'Crème', 'Rouge bordeaux', 'Bleu marine', 'Beige', 'Marron', 'Vert émeraude', 'Rose poudré']
        vintage_eras = ['1950s', '1960s', '1970s', '1980s', '1990s', 'Early 2000s']
        vintage_conditions = ['État neuf', 'Excellent état', 'Très bon état', 'Bon état']
        vintage_sizes = ['XS', 'S', 'M', 'L', 'XL', '34', '36', '38', '40', '42', '44']

        # Distribution équitable des suppliers
        supplier_cycle = list(range(*self.ranges['supplier']))

        for i, product_id in enumerate(self.id_range_iterator('product')):
            supplier_id = supplier_cycle[i % len(supplier_cycle)]

            cost = round(self.fake.random.uniform(80, 800), 2)
            price = round(cost * self.fake.random.uniform(1.8, 4.5), 2)

            yield {
                'product_id': f'P{product_id}',
                'name': f"{self.fake.random_element(vintage_brands)} {self.fake.random_element(vintage_categories)}",
                'category': self.fake.random_element(vintage_categories),
                'subcategory': self.fake.random_element(['Classic', 'Vintage', 'Modern', 'Premium']),
                'brand': self.fake.random_element(vintage_brands),
                'material': self.fake.random_element(vintage_materials),
                'color': self.fake.random_element(vintage_colors),
                'price': price,
                'cost': cost,
                'weight_kg': round(self.fake.random.uniform(0.2, 2.5), 2),
                'dimensions_cm': f"{self.fake.random_int(15, 45)}x{self.fake.random_int(10, 35)}x{self.fake.random_int(5, 20)}",
                'supplier_id': f"SUP{supplier_id:03d}",
                'created_date': self.fake.date_between('-3y', '-1y').isoformat(),
                'last_updated': self.fake.date_between('-1y', 'today').isoformat(),
                'is_active': self.fake.boolean(90),
                'sku': f"VINT-{product_id}-{self.fake.random_int(100, 999)}",

                'size': self.fake.random_element(vintage_sizes),
                'vintage_era': self.fake.random_element(vintage_eras),
                'condition': self.fake.random_element(vintage_conditions),
                'authenticity': self.fake.random_element(['Authentifié', 'Certifié original', 'Expertise validée']),
                'rarity_level': self.fake.random_element(['Commun', 'Rare', 'Très rare', 'Pièce unique']),
                'provenance': self.fake.random_element(['Collection privée', 'Maison de couture', 'Estate sale', 'Archive mode'])
            }

    def generate_customers(self) -> Iterator[dict[str, Any]]:
        """Génère customers collectionneurs fashion vintage - Compatible TOUS ingesters"""
        vintage_interests = ['Haute Couture', 'Prêt-à-Porter', 'Accessoires', 'Chaussures', 'Bijoux vintage']
        customer_types = ['Collectionneur privé', 'Styliste professionnel', 'Influenceur mode', 'Amateur éclairé', 'Revendeur boutique']
        preferred_eras = ['1950s', '1960s', '1970s', '1980s', '1990s', 'Multi-époque']

        for customer_id in self.id_range_iterator('customer'):
            registration_date = self.fake.date_between('-3y', 'today')

            yield {
                'customer_id': f'C{customer_id}',
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'date_of_birth': self.fake.date_of_birth(minimum_age=22, maximum_age=65).isoformat(),
                'gender': self.fake.random_element(['M', 'F', 'Other']),
                'address': json.dumps({  # format string
                    'street': self.fake.street_address(),
                    'city': self.fake.random_element(['Paris', 'Milan', 'London', 'New York', 'Los Angeles', 'Tokyo']),
                    'postal_code': self.fake.postcode(),
                    'country': self.fake.random_element(['France', 'Italy', 'United Kingdom', 'United States', 'Japan'])
                }),
                'segment': self.fake.random_element(['VIP', 'Premium', 'Standard', 'New']),
                'registration_date': registration_date.isoformat(),
                'last_purchase_date': self.fake.date_between('-1y', 'today').isoformat(),
                'total_orders': self.fake.random_int(1, 50),
                'lifetime_value': round(self.fake.random.uniform(500, 50000), 2),
                'preferred_channel': self.fake.random_element(['Online VIP', 'Showroom privé', 'Événements exclusifs']),
                'marketing_consent': self.fake.boolean(80),

                'customer_type': self.fake.random_element(customer_types),
                'vintage_interests': self.fake.random_element(vintage_interests),
                'preferred_era': self.fake.random_element(preferred_eras),
                'collection_size': self.fake.random_int(5, 200),
                'spending_tier': self.fake.random_element(['Bronze', 'Silver', 'Gold', 'Platinum']),
                'loyalty_points': self.fake.random_int(500, 25000),
                'authentication_priority': self.fake.boolean(75)
            }

    def generate_stores(self) -> Iterator[dict[str, Any]]:
        """Génère stores boutiques fashion vintage - Compatible TOUS ingesters"""
        store_types = ['Flagship Vintage', 'Boutique Exclusive', 'Showroom Privé', 'Pop-up Fashion Week', 'Atelier de Collection']
        locations = ['Le Marais, Paris', 'Quadrilatero della Moda, Milan', 'Mayfair, London', 'SoHo, New York', 'Ginza, Tokyo']

        for store_id in self.id_range_iterator('store'):
            location = self.fake.random_element(locations)
            manager_name = self.fake.name()

            yield {
                'store_id': f'ST{store_id}',
                'store_name': f"Vintage Couture {location.split(',')[1].strip()}",
                'manager_name': manager_name,
                'address': json.dumps({  # format string
                    'street': self.fake.street_address(),
                    'district': location.split(',')[0],
                    'city': location.split(',')[1].strip(),
                    'postal_code': self.fake.postcode(),
                    'country': self.fake.random_element(['France', 'Italy', 'United Kingdom', 'United States', 'Japan'])
                }),
                'city': location.split(',')[1].strip(),
                'country': self.fake.random_element(['France', 'Italy', 'United Kingdom', 'United States', 'Japan']),
                'phone': self.fake.phone_number(),
                'email': f"manager.{location.split(',')[1].strip().lower().replace(' ', '')}@vintagecouture.com",
                'opening_date': self.fake.date_between('-15y', '-1y').isoformat(),
                'store_size_sqm': self.fake.random_int(80, 300),
                'is_active': self.fake.boolean(98),

                'type': self.fake.random_element(store_types),
                'location': location,
                'specialization': self.fake.random_element(['Haute Couture', 'Designer Vintage', 'Accessoires de Luxe']),
                'vip_appointment_only': self.fake.boolean(60),
                'authentication_service': self.fake.boolean(90)
            }

    def generate_sales(self) -> Iterator[dict[str, Any]]:
        """Génère sales avec références cohérentes - Compatible ingester_direct"""
        channels = ['Online VIP', 'Boutique', 'Showroom privé', 'Téléphone']
        payment_methods = ['Carte de crédit', 'Virement', 'PayPal', 'Crypto', 'Financement']

        # Créer une liste simplifiée des products pour jointure rapide
        vintage_brands = ['Chanel', 'Dior', 'Yves Saint Laurent', 'Hermès', 'Prada', 'Gucci', 'Versace', 'Valentino']
        vintage_categories = ['Robe', 'Veste', 'Pantalon', 'Jupe', 'Chemise', 'Manteau', 'Blouse', 'Accessoire']

        for sale_id in self.id_range_iterator('sale'):
            sale_date = self.fake.date_between('-2y', 'today')
            quantity = self.fake.random_int(1, 3)
            unit_price = round(self.fake.random.uniform(200, 2500), 2)
            discount_percent = self.fake.random.uniform(0, 15)

            subtotal = round(quantity * unit_price, 2)
            discount_amount = round(subtotal * discount_percent / 100, 2)
            total_amount = round(subtotal - discount_amount, 2)
            tax_amount = round(total_amount * 0.20, 2)  # TVA française

            # Génération du nom de produit cohérent avec l'ID
            product_id_val = self.random_id_from_range("product")
            product_name = f"{self.fake.random_element(vintage_brands)} {self.fake.random_element(vintage_categories)} Vintage"

            yield {
                'sale_id': f'S{sale_id}',
                'customer_id': f'C{self.random_id_from_range("customer")}',
                'product_id': f'P{product_id_val}',
                'product_name': product_name,
                'store_id': f'ST{self.random_id_from_range("store")}',
                'quantity': quantity,
                'unit_price': unit_price,
                'subtotal': subtotal,
                'discount_percent': discount_percent,
                'discount_amount': discount_amount,
                'total_amount': total_amount,
                'tax_amount': tax_amount,
                'sale_date': sale_date.isoformat(),
                'channel': self.fake.random_element(channels),
                'payment_method': self.fake.random_element(payment_methods),
                'country': self.fake.random_element(['France', 'Italy', 'United Kingdom', 'United States', 'Japan']),
                'sales_consultant': self.fake.name(),
                'authentication_verified': self.fake.boolean(95),
                'gift_wrapping': self.fake.boolean(40),
                'notes': self.fake.text(max_nb_chars=100) if self.fake.boolean(25) else None
            }

    def generate_returns(self) -> Iterator[dict[str, Any]]:
        real_sale_range = (100001, 200000)
        real_product_range = (2001, 6000)
        real_customer_range = (1001, 11000)

        for return_id in self.id_range_iterator('return'):
            sale_id = f"S{self.fake.random_int(real_sale_range[0], real_sale_range[1])}"
            product_id = f"P{self.fake.random_int(real_product_range[0], real_product_range[1])}"
            customer_id = f"C{self.fake.random_int(real_customer_range[0], real_customer_range[1])}"

            yield {
                "return_id": f"R{return_id}",
                "sale_id": sale_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "return_date": self.fake.date_between(start_date='-6m', end_date='today').isoformat(),
                "reason": self.fake.random_element([
                    "Defective", "Wrong Size", "Changed Mind", "Damaged in Transit",
                    "Not as Described", "Quality Issues"
                ]),
                "condition": self.fake.random_element(["New", "Like New", "Good", "Fair", "Poor"]),
                "refund_amount": round(self.fake.pyfloat(left_digits=3, right_digits=2, positive=True, min_value=50, max_value=500), 2),
                "refund_method": self.fake.random_element(["Card Refund", "Store Credit", "Exchange"]),
                "processed_by": f"STAFF{self.fake.random_int(1, 20):03d}",
                "status": self.fake.random_element(["Pending", "Approved", "Rejected", "Completed"]),
                "notes": self.fake.text(max_nb_chars=100) if self.fake.boolean(30) else None
            }

    def generate_reviews(self) -> Iterator[dict[str, Any]]:
        """Génère des avis clients sur les produits - Compatible ingester_direct"""
        review_comments = [
            "Excellent produit vintage, qualité exceptionnelle",
            "Très satisfait de cet achat, conforme à la description",
            "Authentique et en parfait état, recommandé",
            "Service impeccable, livraison rapide",
            "Produit magnifique, exactement ce que je cherchais",
            "Qualité premium, investissement parfait",
            "Superbe pièce de collection",
            "Pas déçu de mon achat, très belle qualité"
        ]

        review_titles = [
            "Très satisfait", "Excellent achat", "Parfait", "Recommandé",
            "Superbe qualité", "Authentique", "Collection parfaite", "Top qualité"
        ]

        real_product_range = (2001, 6000)
        real_customer_range = (1001, 11000)

        for review_id in self.id_range_iterator('review'):
            rating = self.fake.random_int(3, 5)

            yield {
                "review_id": f"REV{review_id}",
                "product_id": f"P{self.fake.random_int(real_product_range[0], real_product_range[1])}",
                "customer_id": f"C{self.fake.random_int(real_customer_range[0], real_customer_range[1])}",
                "rating": rating,
                "title": self.fake.random_element(review_titles),
                "comment": self.fake.random_element(review_comments),
                "review_date": self.fake.date_between(start_date='-6m', end_date='today').isoformat(),
                "verified_purchase": self.fake.random_element([True, True, True, False]),
                "helpful_votes": self.fake.random_int(0, 15),
                "status": "Published"
            }

    def generate_inventory(self) -> Iterator[dict[str, Any]]:
        """Génère des données d'inventaire pour les produits - Compatible ingester_direct"""
        real_product_range = (2001, 6000)
        real_store_range = (3001, 3020)

        for inventory_id in self.id_range_iterator('inventory'):
            current_stock = self.fake.random_int(0, 100)
            reserved_stock = self.fake.random_int(0, min(10, current_stock))
            reorder_level = max(5, current_stock // 4)
            max_stock_level = current_stock + self.fake.random_int(20, 80)

            yield {
                "inventory_id": f"INV{inventory_id}",
                "product_id": f"P{self.fake.random_int(real_product_range[0], real_product_range[1])}",
                "store_id": f"ST{self.fake.random_int(real_store_range[0], real_store_range[1])}",
                "current_stock": current_stock,
                "reserved_stock": reserved_stock,
                "reorder_level": reorder_level,
                "max_stock_level": max_stock_level,
                "last_restocked": self.fake.date_between(start_date='-3m', end_date='today').isoformat(),
                "next_delivery_date": self.fake.date_between(start_date='today', end_date='+30d').isoformat(),
                "warehouse_location": f"A{self.fake.random_int(1,10)}-{self.fake.random_int(1,20)}-{self.fake.random_int(1,50)}"
            }

    def generate_promotions(self) -> Iterator[dict[str, Any]]:
        """Génère des promotions fashion vintage - Compatible ingester_snowpipe"""
        promotion_names = [
            "Winter Vintage Sale", "Spring Collection Launch", "VIP Member Exclusive",
            "Chanel Heritage Collection", "Dior Legacy Sale", "Hermès Collector Event",
            "YSL Vintage Revival", "Designer Weekend Sale", "Fashion Week Special",
            "Luxury Consignment Event", "Vintage Bag Festival", "Collector's Choice",
            "Authenticated Luxury Sale", "Timeless Elegance Event", "Rare Finds Sale"
        ]

        promotion_descriptions = [
            "Découvrez notre collection exclusive de pièces vintage authentifiées",
            "Offre spéciale sur une sélection de sacs et accessoires de luxe",
            "Event exclusif réservé à nos membres VIP les plus fidèles",
            "Promotion limitée sur les pièces de collection les plus recherchées",
            "Réduction exceptionnelle sur notre sélection vintage premium"
        ]

        for promotion_id in self.id_range_iterator('promotion'):
            discount_type = self.fake.random_element(["PERCENTAGE", "FIXED_AMOUNT", "BUY_ONE_GET_ONE"])
            discount_value = self.fake.random_int(10, 30) if discount_type == "PERCENTAGE" else self.fake.random_int(50, 200)

            start_date = self.fake.date_between(start_date='-6m', end_date='today')
            end_date = self.fake.date_between(start_date=start_date, end_date='+3m')

            yield {

                "promotion_id": f"PROMO{promotion_id}",
                "name": self.fake.random_element(promotion_names),
                "description": self.fake.random_element(promotion_descriptions),
                "discount_type": discount_type,
                "discount_value": discount_value,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "minimum_purchase": self.fake.random_int(100, 500),
                "is_active": self.fake.boolean(70),
                "created_date": self.fake.date_between(start_date='-1y', end_date='today').isoformat()
            }


    def generate_entity(self, entity_name: str) -> None:
        """Génère et sauvegarde une entité complète"""
        generators = {
            'suppliers': self.generate_suppliers,
            'products': self.generate_products,
            'customers': self.generate_customers,
            'stores': self.generate_stores,
            'sales': self.generate_sales,
            'promotions': self.generate_promotions,
            'returns': self.generate_returns,
            'reviews': self.generate_reviews,
            'inventory': self.generate_inventory
        }

        if entity_name not in generators:
            print(f"❌ Générateur non trouvé pour {entity_name}")
            return

        output_file = self.config.output_dir / f"{entity_name}.json"
        count = getattr(self.config, entity_name)

        if count == 0:
            print(f"⏭️ Skipping {entity_name} (count=0)")
            return

        print(f"🔄 Generating {count} {entity_name}...")

        with open(output_file, 'w') as f:
            for record in generators[entity_name]():
                processed_record = self._process_record(record)
                f.write(json.dumps(processed_record) + '\n')

        print(f"✅ Generated {output_file} ({count:,} records)")

    def _process_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """Traite un enregistrement pour assurer la compatibilité JSON"""
        processed = {}
        for key, value in record.items():
            if isinstance(value, dict):
                processed[key] = json.dumps(value) if key in ['address'] else value
            else:
                processed[key] = value
        return processed

    def generate_all(self) -> None:
        """Génère tous les datasets dans l'ordre optimal"""
        print("🚀 Démarrage génération dataset cohérent...")

        entities = ['suppliers', 'products', 'customers', 'stores', 'promotions', 'sales', 'returns', 'reviews', 'inventory']

        for entity in entities:
            self.generate_entity(entity)

        print("\n🎉 Génération terminée")
        print(f"📊 Dataset cohérent créé dans {self.config.output_dir}")

def main() -> None:
    parser = argparse.ArgumentParser(description='🎯 Générateur de données fashion vintage élégant et cohérent')

    # Arguments de génération
    parser.add_argument('--sales', type=int, default=0, help='Nombre de ventes à générer')
    parser.add_argument('--products', type=int, default=0, help='Nombre de produits à générer')
    parser.add_argument('--customers', type=int, default=0, help='Nombre de clients à générer')
    parser.add_argument('--suppliers', type=int, default=0, help='Nombre de fournisseurs à générer')
    parser.add_argument('--stores', type=int, default=0, help='Nombre de magasins à générer')
    parser.add_argument('--promotions', type=int, default=10, help='Nombre de promotions à générer')
    parser.add_argument('--returns', type=int, default=20, help='Nombre de retours à générer')
    parser.add_argument('--reviews', type=int, default=50, help='Nombre d\'avis à générer')
    parser.add_argument('--inventory', type=int, default=50, help='Nombre d\'inventaires à générer')

    args = parser.parse_args()

    # Configuration
    config = GenerationConfig(
        sales=args.sales,
        products=args.products,
        customers=args.customers,
        suppliers=args.suppliers,
        stores=args.stores,
        promotions=args.promotions,
        returns=args.returns,
        reviews=args.reviews,
        inventory=args.inventory
    )

    generator = DataGenerator(config)
    generator.generate_all()

if __name__ == "__main__":
    main()
