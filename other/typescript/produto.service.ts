import { Injectable } from '@angular/core';
import { BehaviorSubject, from, Observable, of } from 'rxjs';

import {
  BaseProduto,
  CartItem,
  Catalogo,
  CatalogoRequest,
  CatalogoResponse,
  Informacoes,
  Item,
  OfertaCampanha,
  OfertaEscalonado,
  Produto,
  ProdutoAtivador,
  ProdutoDescontoRequest,
  ProdutoDistribuidor,
  ProdutoEscalonado,
  ProdutoRequest,
  ProdutoResponse,
} from '../interfaces';

import { AppService } from './app.service';
import { CarrinhoService } from './carrinho.service';
import { ContextoService } from './contexto.service';
import { DistribuidorService } from './distribuidor.service';
import { FirebaseAnalyticsService } from './firebase-analytics.service';
import { OfertasService } from './ofertas.service';
import { SessaoService } from './sessao.service';
import { UltimosVistosService } from './ultimos-vistos.service';
import { UtilService } from './util.service';

@Injectable({
  providedIn: 'root',
})
export class ProdutoService {
  private _isLoading!: boolean;
  private _request!: ProdutoRequest;
  private _informacoes!: Informacoes;
  private _catalogo!: Catalogo;
  private _catalogo$: BehaviorSubject<Catalogo>;
  private _produtos!: Produto[];
  private _produtos$: BehaviorSubject<Produto[]>;
  private _campaignOffers!: OfertaCampanha[];
  private _steppedOffers!: OfertaEscalonado[];
  private _requestProductDiscount!: ProdutoDescontoRequest;
  private _items!: Item[];
  searchProductTopbar: boolean;
  sizeFilterGroup!: number;
  sizeFilterSubgroup!: number;
  filterActive: boolean;
  filterActiveDiscount: boolean;

  get requestProductDiscount(): ProdutoDescontoRequest {
    return this._requestProductDiscount;
  }

  set requestProductDiscount(requestProductDiscount: ProdutoDescontoRequest) {
    this._requestProductDiscount = requestProductDiscount;
  }

  get isLoading(): boolean {
    return this._isLoading;
  }

  set isLoading(value: boolean) {
    this._isLoading = value;
  }

  get request(): ProdutoRequest {
    return this._request;
  }

  set request(request: ProdutoRequest) {
    this._request = request;
  }

  get catalogo(): Catalogo {
    return this._catalogo;
  }

  set catalogo(catalogo: Catalogo) {
    this._catalogo = catalogo;
    this._catalogo$.next(catalogo);
  }

  get informacoes(): Informacoes {
    return this._informacoes;
  }

  set informacoes(informacoes: Informacoes) {
    this._informacoes = informacoes;
  }

  get produtos(): Produto[] {
    return this._produtos;
  }

  set produtos(produtos: Produto[]) {
    this._produtos = produtos;
    this._produtos$.next(produtos);
  }

  get campaignOffers(): OfertaCampanha[] {
    return this._campaignOffers;
  }

  set campaignOffers(campaignOffers: OfertaCampanha[]) {
    this._campaignOffers = campaignOffers;
  }

  get steppedOffers(): OfertaEscalonado[] {
    return this._steppedOffers;
  }

  set steppedOffers(steppedOffers: OfertaEscalonado[]) {
    this._steppedOffers = steppedOffers;
  }

  get items(): Item[] {
    return this._items;
  }

  set items(items: Item[]) {
    this._items = items;
  }

  constructor(
    private appService: AppService,
    private contextoService: ContextoService,
    private sessaoService: SessaoService,
    private distribuidorService: DistribuidorService,
    private ofertasService: OfertasService,
    private carrinhoService: CarrinhoService,
    private ultimosVistosService: UltimosVistosService,
    private utilService: UtilService,
    private firebaseAnalyticsService: FirebaseAnalyticsService
  ) {
    this._produtos$ = new BehaviorSubject<Produto[]>([]);
    this._catalogo$ = new BehaviorSubject<Catalogo>({
      tipo: [],
    });
    this.campaignOffers = [];
    this.steppedOffers = [];
    this.items = [];
    this.filterActive = false;
    this.searchProductTopbar = false;
    this.filterActiveDiscount = false;
  }

  /**
   * Retorna o preço tabela do produto.
   *
   * @param {Produto} product
   * @param {number} id_distribuidor
   * @return {*}  {number}
   * @memberof ProdutoService
   */
  getProductPriceList(product: Produto, id_distribuidor: number): number {
    let produtoDistribuidor: ProdutoDistribuidor;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    return this.utilService.decimalPrecision(produtoDistribuidor?.preco_tabela, 2) || 0;
  }

  /**
   * Retorna o preço do desconto do produto que esteja dentro da validade.
   *
   * @param {Produto} product
   * @param {number} id_distribuidor
   * @return {*}  {number}
   * @memberof ProdutoService
   */
  getProductDiscountPrice(product: Produto, id_distribuidor: number): number {
    let produtoDistribuidor: ProdutoDistribuidor;
    let iso: string;
    let now: Date;
    let end: Date;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    if (produtoDistribuidor?.desconto) {
      let { desconto, data_final } = produtoDistribuidor.desconto;

      if (data_final) {
        data_final = data_final.slice(0, data_final.toString().indexOf(' '));
        now = new Date();
        end = new Date(data_final);

        return end >= now
          ? this.utilService.decimalPrecision(
              produtoDistribuidor.preco_tabela - produtoDistribuidor.preco_tabela * (desconto / 100),
              2
            )
          : 0;
      }
    }

    return 0;
  }

  /**
   * Retorna o percentual do desconto do produto que esteja dentro da validade.
   *
   * @param {Produto} product
   * @param {number} id_distribuidor
   * @return {*}  {number}
   * @memberof ProdutoService
   */
  getProductDiscountPercent(product: Produto, id_distribuidor: number): number {
    let produtoDistribuidor: ProdutoDistribuidor;
    let now: Date;
    let end: Date;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    if (produtoDistribuidor?.desconto) {
      let { desconto, data_final } = produtoDistribuidor.desconto;
      now = new Date();
      end = new Date(data_final);

      return end >= now ? desconto : 0;
    }

    return 0;
  }

  /**
   * Retorna o valor do estoque do produto.
   *
   * @param {Produto} product
   * @param {number} id_distribuidor
   * @return {*}  {number}
   * @memberof ProdutoService
   */
  getStock(product: Produto, id_distribuidor: number): number {
    let produtoDistribuidor: ProdutoDistribuidor;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    return produtoDistribuidor?.estoque || 0;
  }

  getMultipleOfQuantity(product: Produto, id_distribuidor: number): number {
    let produtoDistribuidor: ProdutoDistribuidor;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    return produtoDistribuidor?.multiplo_venda || 1;
  }

  /**
   * Retorna o catálogo dos produtos: tipos, grupos e subgrupos.
   *
   * @param {number} id_distribuidor
   * @return {*}  {Observable<GrupoProdutosResponse>}
   * @memberof ProdutoService
   */
  getCatalog(request: CatalogoRequest): Observable<CatalogoResponse> {
    return this.appService.handlePostHttpClientByPlataform<CatalogoResponse>(
      this.sessaoService.platform,
      `${this.contextoService.appSettings.API}${this.contextoService.appSettings.URL_CATALOGO}`,
      request
    );
  }

  getProdutos(produtoRequest: ProdutoRequest): Observable<ProdutoResponse> {
    return this.appService.handlePostHttpClientByPlataform<ProdutoResponse>(
      this.sessaoService.platform,
      `${this.contextoService.appSettings.API}${this.contextoService.appSettings.URL_PRODUTO}`,
      produtoRequest
    );
  }

  loadCatalog(): Observable<Catalogo> {
    return this._catalogo$.asObservable();
  }

  loadProduto(): Observable<Produto[] | undefined> {
    return this._produtos$.asObservable();
  }

  itemContainsProduct(dist: number, prd: Produto, arr: Item[]): boolean {
    return arr.some((item: Item) => item.id_distribuidor === dist && item.produto.sku === prd.sku);
  }

  /**
   * Verifica se o produto tem ofertas.
   *
   * @param {(BaseProduto | Produto)} produto
   * @param {number} id_distribuidor
   * @return {*}  {boolean}
   * @memberof ProdutoService
   */
  haveOffer(produto: BaseProduto | Produto, id_distribuidor: number): boolean {
    let produtoDistribuidor: ProdutoDistribuidor;
    let product: Produto = <Produto>produto;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    if (this.utilService.isObjectEmpty(produtoDistribuidor.ofertas)) return false;

    return true;
  }

  /**
   * Verifica se o produto tem ofertas do tipo campanha (bonificado).
   *
   * @param {(BaseProduto | Produto)} produto
   * @param {number} id_distribuidor
   * @return {*}  {boolean}
   * @memberof ProdutoService
   */
  haveCampaing(produto: BaseProduto | Produto, id_distribuidor: number): boolean {
    let produtoDistribuidor: ProdutoDistribuidor;
    let product: Produto = <Produto>produto;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    return produtoDistribuidor.ofertas.hasOwnProperty('bonificada');
  }

  /**
   * Verifica se o produto tem ofertas do tipo escalonada (desconto).
   *
   * @param {(BaseProduto | Produto)} produto
   * @param {number} id_distribuidor
   * @return {*}  {boolean}
   * @memberof ProdutoService
   */
  haveStepped(produto: BaseProduto | Produto, id_distribuidor: number): boolean {
    let produtoDistribuidor: ProdutoDistribuidor;
    let product: Produto = <Produto>produto;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    return produtoDistribuidor.ofertas.hasOwnProperty('escalonada');
  }

  /**
   * Faz a inclusão, alteração e exclusão do produto no carrinho/orçamento. Verifica se o produto tem desconto e/ou oferta e faz os cálculos para aplicar melhor preço.
   *
   * @param {Item} item
   * @memberof ProdutoService
   */
  changeProduct(item: Item): void {
    let id_distribuidor: number;
    if ('distribuidores' in item.produto) {
      id_distribuidor = item.produto.distribuidores[0].id_distribuidor;
    } else {
      id_distribuidor = this.distribuidorService.distribuidor.id_distribuidor;
    }

    let idOffers: number[] = [];
    let idOffer: number | undefined;
    let campaignOffer: OfertaCampanha | undefined;
    let steppedOffer: OfertaEscalonado | undefined;
    let cartItem: CartItem;
    let activatorProduct: ProdutoAtivador;
    let steppedProduct: ProdutoEscalonado;
    let produto: Produto;

    let priceList: number;
    let discountProductPrice: number;
    let discountProductPercent: number;
    let discountPriceInCart: number;
    let discountPercentInCart: number;
    let appliedPriceInCart: number;
    let discountPrice: number;
    let discountPercent: number;
    let appliedPrice: number;

    produto = <Produto>item.produto;

    if (id_distribuidor === 0) {
      id_distribuidor =
        produto.distribuidores != null && produto.distribuidores.length === 1
          ? produto.distribuidores[0].id_distribuidor
          : this.getLowestPriceDistributor(produto.distribuidores);
    }

    priceList = this.getProductPriceList(produto, id_distribuidor);
    discountProductPrice = this.getProductDiscountPrice(produto, id_distribuidor);
    discountProductPercent = this.getProductDiscountPercent(produto, id_distribuidor);
    discountPriceInCart = this.carrinhoService.getDiscountPriceInCart(produto, id_distribuidor);
    discountPercentInCart = this.carrinhoService.getDiscountInCart(produto, id_distribuidor);

    //Verifica o melhor desconto, se o do produto ou o do carrinho (gerado pela oferta)
    discountPrice = this.utilService.getMinValue(discountPriceInCart, discountProductPrice);
    discountPercent = this.utilService.getMinValue(discountPercentInCart, discountProductPercent);

    //Verifica o melor preço a ser aplicado
    appliedPrice = this.utilService.getMinValue(discountPrice, priceList);

    if (this.haveOffer(item.produto, id_distribuidor)) {
      idOffers = this.getOffersNumbers(item, id_distribuidor);

      if (idOffers.length > 1) {
        let itemInCart: CartItem | undefined;
        let lastOfferEntered: number | undefined;

        // Verifica se a última oferta inserida no carrinho está entre as ofertas deste produto
        lastOfferEntered = idOffers.find((offer) => offer === this.ofertasService.lastOfferEntered);

        // Se houver última oferta escolhe ela, se não escolhe a última do array
        if (!!lastOfferEntered) {
          idOffer = lastOfferEntered;
        } else {
          idOffer = [...idOffers].pop();
        }

        // Captura o cartItem do carrinho da oferta escolhida
        if (idOffer) {
          itemInCart = this.carrinhoService.offerExistsInCart(item, idOffer, id_distribuidor);
        }

        // Calcula a quantidade líquida que tem que ser alterada na oferta escolhida
        if (!!itemInCart) {
          item.quantidade =
            item.quantidade - (this.carrinhoService.getQtyTotalProdutct(produto, id_distribuidor) - itemInCart.quantidade);
        }
      } else {
        idOffer = idOffers[0];
      }

      campaignOffer = this.campaignOffers.find((item: OfertaCampanha) => item.id_campanha === idOffer);
      steppedOffer = this.steppedOffers.find((item: OfertaEscalonado) => item.id_escalonado === idOffer);

      if (campaignOffer) {
        this.ofertasService.addCampaignOffers(campaignOffer);

        activatorProduct = campaignOffer.produto_ativador.filter(
          (product: ProdutoAtivador) => product.sku === item.produto.sku
        )[0];

        item.id_campanha = campaignOffer.id_campanha;
        activatorProduct.quantidade = item.quantidade;

        // Calcula as regras de oferta campanha e inclui a quantidade no carrinho.
        this.ofertasService.calcCampaignProducts(activatorProduct, campaignOffer);

        if (idOffer) this.ofertasService.lastOfferEntered = idOffer;
      }

      if (steppedOffer) {
        this.ofertasService.addSteppedOffers(steppedOffer);

        steppedProduct = steppedOffer.produto_escalonado.filter(
          (product: ProdutoEscalonado) => product.sku === item.produto.sku
        )[0];

        item.id_escalonado = steppedOffer.id_escalonado;
        steppedProduct.quantidade = item.quantidade;

        // Calcula as regras de oferta escalonada e inclui a quantidade no carrinho.
        this.ofertasService.calcSteppedProducts(steppedProduct, steppedOffer);

        if (idOffer) this.ofertasService.lastOfferEntered = idOffer;
      }

      // Atualiza a lista de produtos com o melhor desconto e o preço final a ser aplicado.
      this.items.forEach((_item) => {
        // Trata somente o item que está sendo alterado
        if (_item.id_distribuidor === item.id_distribuidor && _item.produto.sku === item.produto.sku) {
          appliedPriceInCart = this.carrinhoService.getAppliedPriceInCart(produto, id_distribuidor);
          //Verifica o melhor desconto, se o do produto ou o do carrinho (gerado pela oferta)
          discountPriceInCart = this.carrinhoService.getDiscountPriceInCart(produto, id_distribuidor);
          discountPercentInCart = this.carrinhoService.getDiscountInCart(produto, id_distribuidor);
          discountPrice = this.utilService.getMinValue(discountPriceInCart, discountProductPrice);
          discountPercent = this.utilService.getMinValue(discountPercentInCart, discountProductPercent);

          //Verifica o melor preço a ser aplicado
          appliedPrice = this.utilService.getMinValue(discountPrice, priceList);

          //Verifica se oferta escalonada é unificada e aplica o desconto correto
          if (steppedOffer?.unificada) {
            _item.preco_desconto = discountPriceInCart;
            _item.desconto = discountPercentInCart;
            _item.preco_aplicado = appliedPriceInCart;
          } else {
            _item.preco_desconto = discountPrice;
            _item.desconto = discountPercent;
            _item.preco_aplicado = appliedPrice;
          }
        }

        // Trata os demais itens que fazem parte da mesma oferta escalonada e não estão no carrinho
        if (
          _item.id_distribuidor === item.id_distribuidor &&
          _item.id_escalonado === item.id_escalonado &&
          _item.produto.sku !== item.produto.sku
        ) {
          if (steppedOffer?.unificada) {
            _item.desconto = this.ofertasService.getDiscountOfUnifiedSteppedOffer(steppedProduct, steppedOffer);
            _item.preco_desconto = this.ofertasService.getDiscountPriceOfUnifiedSteppedOffer(steppedProduct, steppedOffer);
          }
        }
      });
    } else {
      item.preco_desconto = discountPrice;
      item.desconto = discountPercent;
      item.preco_aplicado = appliedPrice;

      cartItem = this.carrinhoService.convertItemToCartItem(item, id_distribuidor);

      if (item.quantidade <= 0) {
        this.carrinhoService.delItem(cartItem);
      } else {
        this.carrinhoService.addItem(cartItem);
      }
    }
  }

  /**
   * Separa os tipos de ofertas.
   *
   * @memberof ProductListComponent
   */
  separateOffers() {
    this.campaignOffers = [];
    this.steppedOffers = [];

    this.ofertasService.offers.forEach((offer) => {
      if (this.ofertasService.isCampaignObject(offer)) {
        this.campaignOffers.push(offer);
      }
      if (this.ofertasService.isSteppedObject(offer)) {
        this.steppedOffers.push(offer);
      }
    });
  }

  changeQty(item: Item): void {
    if (item.quantidade > item.estoque) item.quantidade = item.estoque;

    let { multiplo_venda, quantidade } = item;

    item.quantidade = this.utilService.calcRoundMultipleOfQuantity(multiplo_venda, quantidade);

    this.ultimosVistosService.addLastSeen(<Produto>item.produto);
  }

  addQty(item: Item): void {
    if (item.quantidade >= item.estoque) return;

    let cartItem: CartItem;
    let { multiplo_venda, quantidade } = item;

    item.quantidade = quantidade + multiplo_venda;

    this.changeQty(item);

    cartItem = this.carrinhoService.convertItemToCartItem(item, <number>item.id_distribuidor);
    this.firebaseAnalyticsService.addCart(cartItem);
  }

  delQty(item: Item): void {
    if (item.quantidade < 0) item.quantidade = 0;
    if (item.quantidade === 0) return;

    let cartItem: CartItem;
    let { multiplo_venda, quantidade } = item;

    item.quantidade = quantidade - multiplo_venda;
    this.changeQty(item);

    cartItem = this.carrinhoService.convertItemToCartItem(item, <number>item.id_distribuidor);
    this.firebaseAnalyticsService.removeCart(cartItem);
  }

  /**
   * Retorna um array com o número das ofertas.
   *
   * @private
   * @param {Item} item
   * @return {*}  {number[]}
   * @memberof ProductListComponent
   */
  private getOffersNumbers(item: Item, id_distribuidor: number): number[] {
    let product: Produto;
    let produtoDistribuidor: ProdutoDistribuidor;
    let offers: number[] = [];

    product = <Produto>item.produto;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    if (produtoDistribuidor.ofertas.bonificada && produtoDistribuidor.ofertas.bonificada.produto_ativador) {
      offers = [...offers, ...produtoDistribuidor.ofertas.bonificada?.produto_ativador];
    }
    if (produtoDistribuidor.ofertas.escalonada && produtoDistribuidor.ofertas.escalonada.produto_escalonado) {
      offers = [...offers, ...produtoDistribuidor.ofertas.escalonada.produto_escalonado];
    }

    return offers;
  }

  private getLowestPriceDistributor(distributors: ProdutoDistribuidor[]): number {
    let produtoDistribuidor: ProdutoDistribuidor;

    //TODO: Verificar como será com será o cálculo com os produtos com desconto
    // distributors.forEach(product => {
    //   let price = 0;

    // });

    if (!distributors.length) return 0;

    produtoDistribuidor = distributors.reduce((prev, curr) => (prev.preco_tabela < curr.preco_tabela ? prev : curr));

    return produtoDistribuidor.id_distribuidor;
  }

  notifyProduct(id_produto: string): Observable<boolean> {
    return of(true); //TODO: Carla, alterar quando API estiver desenvolvida
    // return this.appService.handlePostHttpClientByPlataform<any>(
    //   this.sessaoService.platform,
    //   `${this.contextoService.appSettings.API}${this.contextoService.appSettings.URL_NOTIFICAR_PRODUTO}`,
    //   {
    //     id_produto: id_produto,
    //   }
    // );
  }

  getProductsDiscount(requestProductDiscount: ProdutoDescontoRequest): Observable<any> {
    this.isLoading = true;
    return this.appService.handlePostHttpClientByPlataform<ProdutoResponse>(
      this.sessaoService.platform,
      `${this.contextoService.appSettings.API}${this.contextoService.appSettings.URL_OFERTAS_DESCONTO}`,
      requestProductDiscount
    );
  }
}
