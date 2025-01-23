import { Injectable } from '@angular/core';
import { BehaviorSubject, Observable, of } from 'rxjs';
import { debounceTime, distinctUntilChanged, switchMap } from 'rxjs/operators';

import {
  Carrinho,
  CarrinhoResponse,
  DefaultResponse,
  Item,
  CartItem,
  MetodoPagamentoDistribuidor,
  OfertaCampanha,
  OfertaEscalonado,
  Produto,
  ProdutoAtivador,
  ProdutoDistribuidor,
  ProdutoEscalonado,
  ResumoPedidos,
  MetodoPagamento,
  OrcamentosRequest,
  OrcamentosResponse,
  Orcamento,
  OrcamentosDistribuidor,
} from '../interfaces';

import { AppService } from './app.service';
import { ClienteService } from './cliente.service';
import { ContextoService } from './contexto.service';
import { SessaoService } from './sessao.service';
import { UsuarioService } from './usuario.service';
import { UtilService } from './util.service';

@Injectable({
  providedIn: 'root',
})
export class CarrinhoService {
  private _isLoading$!: BehaviorSubject<boolean>;
  private _carrinho?: Carrinho;
  private _orcamentos?: Orcamento[];
  private _orcamentosDistribuidor?: OrcamentosDistribuidor;
  private _metodoPagamento!: MetodoPagamentoDistribuidor;
  private _resumoPedidos?: ResumoPedidos;
  private _totalItems!: number;

  private _carrinho$: BehaviorSubject<Carrinho | undefined>;
  private _orcamentos$: BehaviorSubject<Orcamento[] | undefined>;

  get isLoading(): boolean {
    return this._isLoading$.value;
  }

  set isLoading(value: boolean) {
    this._isLoading$.next(value);
  }

  get carrinho(): Carrinho | undefined {
    return this._carrinho;
  }

  set carrinho(cart: Carrinho | undefined) {
    this._carrinho = cart;
    this._carrinho$.next(cart);
  }

  get orcamentos(): Orcamento[] | undefined {
    return this._orcamentos;
  }

  set orcamentos(orcamentos: Orcamento[] | undefined) {
    this._orcamentos = orcamentos;
    this._orcamentos$.next(orcamentos);
  }

  get orcamentosDistribuidor(): OrcamentosDistribuidor | undefined {
    return this._orcamentosDistribuidor;
  }

  set orcamentosDistribuidor(orcamento: OrcamentosDistribuidor | undefined) {
    this._orcamentosDistribuidor = orcamento;
  }

  get metodoPagamento(): MetodoPagamentoDistribuidor {
    return this._metodoPagamento;
  }

  set metodoPagamento(metodoPagamento: MetodoPagamentoDistribuidor) {
    this._metodoPagamento = metodoPagamento;
  }

  get resumoPedidos(): ResumoPedidos | undefined {
    return this._resumoPedidos;
  }

  set resumoPedidos(resumoPedidos: ResumoPedidos | undefined) {
    this._resumoPedidos = resumoPedidos;
  }

  get totalItems(): number {
    return this._totalItems || 0;
  }

  set totalItems(value: number) {
    this._totalItems = value;
  }

  constructor(
    private appService: AppService,
    private contextoService: ContextoService,
    private clienteService: ClienteService,
    private sessaoService: SessaoService,
    private usuarioService: UsuarioService,
    private utilService: UtilService
  ) {
    this._isLoading$ = new BehaviorSubject<boolean>(false);
    this._carrinho$ = new BehaviorSubject<Carrinho | undefined>(undefined);
    this._orcamentos$ = new BehaviorSubject<Orcamento[] | undefined>(undefined);
  }

  getIsLoading(): Observable<boolean> {
    return this._isLoading$.asObservable();
  }

  offerExistsInCart(item: Item, id_offer: number, id_distributor: number): CartItem | undefined {
    if (!this.carrinho) return;
    let itemInCart!: CartItem | undefined;

    itemInCart = this.carrinho.itens.find(
      (cartItem: CartItem) =>
        cartItem.sku === item.produto.sku &&
        cartItem.id_distribuidor === id_distributor &&
        (cartItem.id_campanha === id_offer || cartItem.id_escalonado === id_offer)
    );

    if (itemInCart?.bonificado) return;

    return itemInCart;
  }

  addItem(cartItem: CartItem): void {
    if (!this.usuarioService.usuario) return;

    if (cartItem.preco_desconto) cartItem.preco_desconto = this.utilService.decimalPrecision(cartItem.preco_desconto, 2);
    // Verifica se o carrinho existe.
    if (this.carrinho) {
      let i!: number;
      // Verifica se o produto existe no carrinho do distribuidor
      if (
        this.carrinho.itens?.some((item: CartItem, index: number) => {
          if (
            cartItem.sku === item.sku &&
            cartItem.id_distribuidor === item.id_distribuidor &&
            cartItem.id_campanha === item.id_campanha &&
            cartItem.id_escalonado === item.id_escalonado &&
            cartItem.bonificado === item.bonificado
          ) {
            i = index;
            return true;
          }
          return false;
        })
      ) {
        // Se existir atualiza a quantidade
        if (i >= 0) this.carrinho.itens[i] = cartItem;
      } else {
        // Se não existir inclui o produto
        this.carrinho.itens?.push(cartItem);
      }

      this.carrinho = {
        ...this.carrinho,
      };
    } else {
      // Se o carrinho não existir cria o carrinho com o item.
      this.carrinho = {
        id_cliente: this.clienteService.cliente.id_cliente,
        itens: [cartItem],
      };
    }
  }

  delItem(cartItem: CartItem): void {
    if (!this.usuarioService.usuario) return;
    if (!this.carrinho) return;

    let i!: number;

    if (
      this.carrinho.itens?.some((item: CartItem, index: number) => {
        if (
          item.sku === cartItem.sku &&
          item.id_distribuidor === cartItem.id_distribuidor &&
          cartItem.id_campanha === item.id_campanha &&
          cartItem.id_escalonado === item.id_escalonado &&
          cartItem.bonificado === item.bonificado
        ) {
          i = index;
          return true;
        }
        return false;
      })
    ) {
      if (cartItem.quantidade > 0) {
        this.carrinho.itens[i] = cartItem;
      } else {
        this.carrinho.itens = this.carrinho.itens.filter((f, index) => index !== i);
      }
    }

    this.carrinho = {
      ...this.carrinho,
    };
  }

  savePayment(id_distributor: number, paymentMethod: MetodoPagamento) {
    if (!this.usuarioService.usuario) return;

    if (this.metodoPagamento && this.metodoPagamento[id_distributor]) {
      this.metodoPagamento[id_distributor] = paymentMethod;
    } else if (this.metodoPagamento) {
      this.metodoPagamento = {
        ...this.metodoPagamento,
        [id_distributor]: paymentMethod,
      };
    } else {
      this.metodoPagamento = { [id_distributor]: paymentMethod };
    }
  }

  getQtyProdutct(sku: string, id_distribuidor: number): number {
    if (!this.usuarioService.usuario) return 0;
    if (!this.carrinho) return 0;

    let cartItem: CartItem | undefined;

    cartItem = this.carrinho.itens.find(
      (item: CartItem) =>
        item.sku === sku && item.id_distribuidor === id_distribuidor && !item.id_campanha && !item.id_escalonado
    );

    return cartItem?.quantidade || 0;
  }

  getQtyTotalProdutct(product: Produto, id_distribuidor: number): number {
    if (!this.usuarioService.usuario) return 0;
    if (!this.carrinho) return 0;

    let total: number = 0;
    let produtoDistribuidor: ProdutoDistribuidor;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    this.carrinho.itens.forEach((item: CartItem) => {
      if (item.sku === product.sku && item.id_distribuidor === produtoDistribuidor.id_distribuidor && !item.bonificado) {
        total = total + item.quantidade;
      }
    });

    return total;
  }

  getQtyProdutctOffer(product: ProdutoAtivador | ProdutoEscalonado): number {
    if (!this.usuarioService.usuario) return 0;
    if (!this.carrinho) return 0;

    let cartItem: CartItem | undefined;

    if (this.isCampaignObject(product)) {
      cartItem = this.carrinho.itens.find(
        (item: CartItem) =>
          item.id_distribuidor === product.id_distribuidor &&
          item.id_campanha === product.id_campanha &&
          item.sku === product.sku &&
          !item.bonificado
      );
    }

    if (this.isSteppedObject(product)) {
      cartItem = this.carrinho.itens.find(
        (item: CartItem) =>
          item.id_distribuidor === product.id_distribuidor &&
          item.id_escalonado === product.id_escalonado &&
          item.sku === product.sku
      );
    }

    return cartItem?.quantidade || 0;
  }

  getDiscountPriceInCart(product: Produto, id_distribuidor: number): number {
    if (!this.usuarioService.usuario) return 0;
    if (!this.carrinho) return 0;

    let price: number = 0;
    let produtoDistribuidor: ProdutoDistribuidor;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    this.carrinho.itens.forEach((item: CartItem) => {
      if (item.sku === product.sku && item.id_distribuidor === produtoDistribuidor.id_distribuidor && !item.bonificado) {
        price = (item.preco_desconto && this.utilService.decimalPrecision(item.preco_desconto, 2)) || 0;
      }
    });

    return price;
  }

  getDiscountInCart(product: Produto, id_distribuidor: number): number {
    if (!this.usuarioService.usuario) return 0;
    if (!this.carrinho) return 0;

    let discount: number = 0;
    let produtoDistribuidor: ProdutoDistribuidor;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    this.carrinho.itens.forEach((item: CartItem) => {
      if (item.sku === product.sku && item.id_distribuidor === produtoDistribuidor.id_distribuidor && !item.bonificado) {
        discount = item.desconto || 0;
      }
    });

    return discount;
  }

  getAppliedPriceInCart(product: Produto, id_distribuidor: number): number {
    if (!this.usuarioService.usuario) return 0;
    if (!this.carrinho) return 0;
    if (!id_distribuidor) return 0;

    let price: number = 0;
    let produtoDistribuidor: ProdutoDistribuidor;

    if (id_distribuidor === 0) {
      produtoDistribuidor = product.distribuidores[0];
    } else {
      produtoDistribuidor = product.distribuidores.filter((d) => d.id_distribuidor === id_distribuidor)[0];
    }

    this.carrinho.itens.forEach((item: CartItem) => {
      if (item.sku === product.sku && item.id_distribuidor === produtoDistribuidor.id_distribuidor && !item.bonificado) {
        price = item.preco_aplicado || 0;
      }
    });

    return price;
  }

  convertItemToCartItem(item: Item, id_distribuidor: number): CartItem {
    let cartItem: CartItem;
    let { id_tipo, id_grupo, id_subgrupo, id_campanha, id_escalonado, produto } = item;
    let { id_produto, id_marca, distribuidores } = <Produto>produto || {};
    let produtoDistribuidor: ProdutoDistribuidor;

    cartItem = {
      id_distribuidor,
      id_campanha,
      id_escalonado,
      id_produto,
      id_marca,
      id_tipo,
      id_grupo,
      id_subgrupo,
      preco_tabela: this.utilService.decimalPrecision(item.preco_tabela, 2),
      preco_desconto: item.preco_desconto && this.utilService.decimalPrecision(item.preco_desconto, 2),
      preco_aplicado: this.utilService.decimalPrecision(item.preco_aplicado, 2),
      multiplo_venda: item.multiplo_venda,
      quantidade: item.quantidade,
      estoque: item.estoque,
      desconto: item.desconto || 0,
      ...this.utilService.pick(produto, [
        'sku',
        'descricao_produto',
        'descricao_marca',
        'imagem',
        'status',
        'unidade_embalagem',
        'quantidade_embalagem',
        'unidade_venda',
        'quant_unid_venda',
      ]),
    };

    if (distribuidores) {
      produtoDistribuidor = distribuidores.filter(
        (product: ProdutoDistribuidor) => product.id_distribuidor === id_distribuidor
      )[0];

      let { id_produto, id_tipo, id_grupo, id_subgrupo, multiplo_venda } = produtoDistribuidor;

      cartItem = {
        ...cartItem,
        id_produto,
        multiplo_venda: multiplo_venda,
        id_tipo,
        id_grupo,
        id_subgrupo,
      };
    } else if (id_produto) {
      cartItem = {
        ...cartItem,
        id_produto,
      };
    }

    return cartItem;
  }

  loadCart(): Observable<Carrinho | undefined> {
    return this._carrinho$.asObservable();
  }

  loadCartUntilChanged(): Observable<Carrinho | undefined> {
    return this._carrinho$.asObservable().pipe(
      debounceTime(5000),
      distinctUntilChanged(),
      switchMap((carrinho) => of(carrinho))
    );
  }

  loadBudget(): Observable<Orcamento[] | undefined> {
    return this._orcamentos$.asObservable();
  }

  loadBudgetUntilChanged(): Observable<Orcamento[] | undefined> {
    return this._orcamentos$.asObservable().pipe(
      debounceTime(5000),
      distinctUntilChanged(),
      switchMap((orcamentos) => of(orcamentos))
    );
  }

  generateBudgetBasket() {
    if (this.carrinho?.itens?.length) {
      const unique_ids = [...new Set(this.carrinho.itens.map((item) => item.id_distribuidor))];

      if (!this.orcamentosDistribuidor) {
        this.orcamentosDistribuidor = {};
      }

      if (this.orcamentosDistribuidor) {
        unique_ids.forEach((id_distribuidor) => {
          // zera os itens dos orçamentos existentes
          if (this.orcamentosDistribuidor && this.orcamentosDistribuidor[id_distribuidor]) {
            this.orcamentosDistribuidor = {
              ...this.orcamentosDistribuidor,
              [id_distribuidor]: {
                ...this.orcamentosDistribuidor[id_distribuidor],
                id_distribuidor,
                itens: [],
              },
            };
          }
          // monta os orçamentos zerados se não existe
          else if (this.orcamentosDistribuidor) {
            this.orcamentosDistribuidor = {
              ...this.orcamentosDistribuidor,
              [id_distribuidor]: {
                id_distribuidor,
                itens: [],
              },
            };
          }
        });
        // inclui os itens do carrinho nos orçamentos
        this.carrinho.itens.forEach((cartItem: CartItem) => {
          const id_distribuidor = cartItem.id_distribuidor;

          this.orcamentosDistribuidor && this.orcamentosDistribuidor[id_distribuidor].itens.push(cartItem);
        });
        // remove o orçamento não tem itens no carrinho
        Object.keys(this.orcamentosDistribuidor).forEach((key) => {
          const id_distribuidor = parseInt(key);
          if (!unique_ids.includes(id_distribuidor)) {
            this.orcamentosDistribuidor && delete this.orcamentosDistribuidor[id_distribuidor];
          }
        });
      }
    } else {
      this.orcamentosDistribuidor = {};
    }
  }

  generateBudgets() {
    this.orcamentos = [];

    if (this.orcamentosDistribuidor) {
      Object.keys(this.orcamentosDistribuidor).forEach((dist: string) => {
        const id_distribuidor: number = parseInt(dist);

        if (this.orcamentosDistribuidor && this.orcamentosDistribuidor[id_distribuidor]) {
          let orcamento: Orcamento;
          let { id_cupom, itens } = this.orcamentosDistribuidor[id_distribuidor];

          orcamento = {
            id_distribuidor,
            itens,
          };

          if (id_cupom)
            orcamento = {
              ...orcamento,
              id_cupom,
            };

          this.orcamentos?.push(orcamento);
        }
      });
    }

    this.calculateTotalIemsInCart();
    // debugger;
  }

  getCart(id_client: number): Observable<CarrinhoResponse> {
    return this.appService.handleGetHttpClientByPlataform<CarrinhoResponse>(
      this.sessaoService.platform,
      `${this.contextoService.appSettings.API}${this.contextoService.appSettings.URL_CARRINHO}/${id_client}`
    );
  }

  saveCart(carrinho: Carrinho | undefined): Observable<DefaultResponse> {
    return this.appService.handlePostHttpClientByPlataform<DefaultResponse>(
      this.sessaoService.platform,
      `${this.contextoService.appSettings.API}${this.contextoService.appSettings.URL_CARRINHO}`,
      carrinho
    );
  }

  getBudget(id_client: number): Observable<OrcamentosResponse> {
    return this.appService.handleGetHttpClientByPlataform<OrcamentosResponse>(
      this.sessaoService.platform,
      `${this.contextoService.appSettings.API}${this.contextoService.appSettings.URL_ORCAMENTO}/${id_client}`
    );
  }

  saveBudget(orcamentosRequest: OrcamentosRequest): Observable<OrcamentosResponse> {
    return this.appService.handlePostHttpClientByPlataform<OrcamentosResponse>(
      this.sessaoService.platform,
      `${this.contextoService.appSettings.API}${this.contextoService.appSettings.URL_ORCAMENTO}`,
      orcamentosRequest
    );
  }

  /**
   * Calcula e salva a quantidade de itens do carrinho.
   *
   * @private
   * @memberof CarrinhoService
   */
  calculateTotalIemsInCart(): void {
    if (this.carrinho && this.carrinho.itens) this.totalItems = this._carrinho?.itens.length || 0;
  }

  /**
   * Verifica se o objeto é do tipo OfertaCampanha.
   *
   * @private
   * @param {Object} obj
   * @return {*}  {obj is OfertaCampanha}
   * @memberof OffersListComponent
   */
  private isCampaignObject(obj: Object): obj is OfertaCampanha {
    if ('id_campanha' in obj) {
      let _obj = <OfertaCampanha>obj;
      return !!_obj.id_campanha;
    }
    return false;
  }

  /**
   * Verifica se o objeto é do tipo OfertaEscalonado.
   *
   * @private
   * @param {Object} obj
   * @return {*}  {obj is OfertaEscalonado}
   * @memberof OffersListComponent
   */
  private isSteppedObject(obj: Object): obj is OfertaEscalonado {
    if ('id_escalonado' in obj) {
      let _obj = <OfertaEscalonado>obj;
      return !!_obj.id_escalonado;
    }
    return false;
  }
}
