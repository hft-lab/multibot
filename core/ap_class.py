# class Side:
#     def __init__(self, client, exchange, market, fee, price_parser, max_amount_parser, ob):
#         self.client = client
#         self.exchange = exchange
#         self.market = market
#         self.fee = fee
#         self.price_parser = price_parser
#         self.max_amount_parser = max_amount_parser
#         #
#         self.ob = None  # Заполняется после обновления ордербука
#         self.max_amount_final = None  # Максимально возможный размер ордера в крипте. Заполняется после обновления ордербука
#         self.limit_px = None  # Итоговое целевое значение цены ордера
#         self.buy_size = None  # Итоговый размер ордера в крипте
#
#         self.buy_exchange_order_id = None
#         self.buy_order_id = None
#         self.buy_order_place_time = None
class AP:
    def __init__(self, ap_id):
        # general data section
        self.ap_id = ap_id
        self.coin = None
        self.deal_direction = None
        self.target_profit = None
        self.deal_max_amount_parser = None
        self.deal_max_usd_parser = None
        self.deal_max_amount_ob = None
        self.deal_max_usd_ob = None
        self.deal_size_amount_target = None
        self.deal_size_usd_target = None

        self.profit_rel_parser = None
        self.profit_rel_target = None
        self.profit_usd_parser_max = None
        self.profit_usd_target = None



        # self.buy: Side
        # self.sell: Side

        # time section
        self.dt_create_ap = None
        self.ts_create_ap = None
        self.ts_choose_end = None
        self.ts_define_potential_deals_end = None
        self.ts_check_still_good_end = None
        self.ts_orders_sent = None  # Момент отправки ордеров
        self.ts_orders_responses_received = None # Момент получения ответа по ордерам
        self.time_parser = None  # Длительность цикла парсингка
        self.time_define_potential_deals = None  # Длительность анализа данных из парсера и определения потенциальных AP
        self.time_choose = None  # Длительность выбора лучшей AP
        self.time_check_ob = None  # Длительность контрольного запроса OB



        # BUY side
        self.client_buy = None
        self.ob_buy = None  # Обновленный ордербук
        self.buy_exchange = None
        self.buy_market = None
        self.buy_fee = None

        self.buy_max_amount_parser = None
        self.buy_max_amount_ob = None  # Объем в крипте первого ордера из обновленного OB
        self.buy_amount_target = None  # Итоговый размер ордера в крипте
        self.buy_amount_real = None  # TBD

        self.buy_price_parser = None
        self.buy_price_target = None
        self.buy_price_shifted = None
        self.buy_price_fitted = None  # Итоговое целевое значение цены ордера
        self.buy_price_real = None #TBD


        self.buy_order_id_exchange = None
        self.buy_order_id = None
        self.buy_order_place_time = None

        # SELL side
        self.client_sell = None
        self.ob_sell = None
        self.sell_exchange = None
        self.sell_market = None
        self.sell_fee = None

        self.sell_max_amount_parser = None
        self.sell_max_amount_ob = None
        self.sell_amount_target = None
        self.sell_amount_real = None

        self.sell_price_parser = None
        self.sell_price_target = None
        self.sell_price_shifted = None
        self.sell_price_fitted = None
        self.sell_price_real = None

        self.sell_order_id_exchange = None
        self.sell_order_id = None
        self.sell_order_place_time = None

    # def set_data_from_ob_update(self):
    #     pass

    def set_data_from_parser(self, coin, deal_direction, deal_max_amount_parser, deal_max_usd_parser,
                             expect_profit_rel, profit_usd_max,
                             datetime, timestamp, target_profit):
        self.ts_create_ap = timestamp
        self.dt_create_ap = datetime
        self.coin = coin
        self.deal_direction = deal_direction
        self.target_profit = target_profit
        self.deal_max_amount_parser = deal_max_amount_parser
        self.deal_max_usd_parser = deal_max_usd_parser
        self.profit_rel_parser = expect_profit_rel
        self.profit_usd_parser_max = profit_usd_max

    def set_side_data_from_parser(self, side, client, exchange, market, fee, price, max_amount):
        if side == 'buy':
            self.client_buy = client
            self.buy_exchange = exchange
            self.buy_market = market
            self.buy_fee = fee
            self.buy_max_amount_parser = max_amount
            self.buy_price_parser = price
        if side == 'sell':
            self.client_sell = client
            self.sell_exchange = exchange
            self.sell_market = market
            self.sell_fee = fee
            self.sell_max_amount_parser = max_amount
            self.sell_price_parser = price

    # def set_sell_side_parser(self):
    #     pass
